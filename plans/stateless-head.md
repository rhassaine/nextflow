# Restartable ("stateless") head job — parked plan

- Status: **Parked / draft** (AI-assisted ideation, not yet validated against a prototype)
- Date: 2026-10-16
- Related: PR #6919 (`terminationReason`, `retryOn`) — prerequisite for classifying infra failures

> Code references below were taken from branch `k8s-failure-classification` @ `3d98ba5e2`
> (on top of 26.10.2). Line numbers will drift; verify before acting.

## 1. Problem

Nextflow runs as a long-lived head process that holds the whole run in memory: channel queues,
operator accumulators (e.g. `groupTuple` buckets), task counters, retry attempts, the pending and
running task queues, and the only copy of external job IDs (`K8sTaskHandler.podName`,
`GridTaskHandler.jobId`).

If the head dies:

- in-flight tasks are **resubmitted** on `-resume` (only the task hash is written at submit time:
  `Session.groovy:1246` → `CacheDB.putIndexAsync` → `CacheStore.writeIndex(hash, cached)`);
- on graceful shutdown (e.g. SIGTERM from a pod eviction) running tasks are **killed**
  (`TaskPollingMonitor.cleanup()`, `TaskPollingMonitor.groovy:781`, kill at `:799`);
- on SIGKILL, tasks keep running with nobody watching them (runaway risk on cloud).

## 2. Goal and non-goals

**Goal:** the head becomes a disposable, replaceable reconciler. It can be killed at any time
and a new one picks the run up — without losing work, without duplicating work, and **without
runaway jobs**.

**Non-goals:**

- A truly headless engine (no per-run process). That requires serialisable operator state and a
  new closure evaluator — effectively a rewrite. See §8.
- Changing the Nextflow language or the dataflow semantics.

## 3. Scope: cloud / K8s first

This is primarily a cloud concern:

- runaway jobs cost money (HPC: fairshare hours, bounded by walltime);
- K8s routinely kills the head (evictions, autoscaler drains, upgrades, spot reclaim);
- cloud runs are often automated/multi-tenant with nobody watching;
- HPC schedulers already own job cleanup (`scancel`, walltime).

HPC does benefit from one piece — **reattach after head restart** (long queue waits, head
walltime limits, login-node killers). So:

| Mechanism | Scope |
|---|---|
| Deterministic job names, idempotent submit, `attach` primitive, reconcile-by-listing | Executor-neutral (core) |
| Run CRD, ownerReference GC, lease + epoch fencing, explicit drain/cancel states | K8s-native (nf-k8s or a separate operator) |
| Dead-man's switch heartbeat in the task wrapper | Dropped (walltime on HPC, ownerRefs on K8s) |

## 4. Core idea: the head owns nothing

```
desired   = replay(script, completed facts)
observed  = list(executor jobs, label: run id)
actions   = reconcile(desired, observed)   → launch / attach / kill
```

| Fact | Source of truth | Today |
|---|---|---|
| Completed tasks | Work dir (`.exitcode` + outputs); cache DB is an index over it | Yes |
| In-flight tasks | The executor itself (pods / jobs) | Pods already labelled `nextflow.io/sessionId`, `nextflow.io/runName` (`K8sTaskHandler.groovy:309-313`), never queried |
| What should run next | Script replay | Yes (`-resume`) |
| Ownership, user intent | **Run object** | New |

No write-ahead journal of submissions is needed: the cluster *is* the journal for in-flight work.

## 5. Mechanisms

1. **Run as a durable object (K8s CRD `NextflowRun`).** Spec (pipeline, revision, params, config
   digest), desired state (`Running | Draining | Cancelled`), lease, epoch, status summary.
   `nextflow run` = create Run + follow logs; `nextflow cancel` = set desired state. Cancel works
   with no head alive.
2. **Ownership graph.** Task pods get an `ownerReference` to the Run (today they have none).
   Deleting the Run cascades via the K8s garbage collector. No ownerless tasks by construction.
3. **Lease + fencing epochs.** Only the lease holder acts; each acquisition bumps the epoch.
   Submissions and cache writes carry the epoch and are rejected if stale (CAS on the Run). A
   partitioned zombie head can neither submit nor corrupt state.
4. **Deterministic task identity.** Job name `nf-<runId>-<hash>-<attempt>` (today a random
   time-based suffix: `K8sTaskHandler.getSyntheticPodName`, `K8sTaskHandler.groovy:172`).
   Submission becomes create-if-absent; `AlreadyExists` → attach. Never reuse a work dir: a
   relaunch is always a new attempt in a new directory.
5. **Explicit intent instead of guessing on signals:**

```
            ┌──────── head crash / eviction ────────┐
            ▼                                        │
Pending → Running ──(lease lost)──→ Recovering ──(new head, epoch+1)──→ Running
            ├── nextflow drain  → Draining (no new launches, running finish) → Paused
            └── nextflow cancel → Cancelled (lease holder kills all; else ownerRef GC)
```

SIGTERM = detach (someone will take over). Cancel = kill, with or without a head.

## 6. The hard part: claiming observed jobs (fixpoint)

After a restart, replay is incremental. A running pod not yet reached by replay is **not** an
orphan, so unclaimed jobs cannot be killed eagerly.

- Every observed job starts *unclaimed*; replay claims (attaches) as it reaches them.
- When the dataflow network reaches **quiescence** (every runnable decision blocked on in-flight
  tasks, pending queue empty), anything still unclaimed is a true orphan — typically caused by
  non-deterministic hashing.
- Policy: **strict** (default) kill orphans and report the hash drift; **lenient** let them finish
  and keep outputs as cache candidates.

Open risk: detecting "quiescent but not finished" in GPars may not be cheap. Nextflow detects full
termination today; this is a stricter condition. **Spike this first** — the design stands or
falls on it.

## 7. Phased plan

| Phase | Content | Rough effort |
|---|---|---|
| 0 | Chaos acceptance test: ~10k-task pipeline on K8s, SIGKILL head every N minutes; pass = identical outputs, zero duplicate submissions, zero orphans, bounded recovery time. Establish baseline. | 1–2 wk |
| 1 | Spike the quiescence/fixpoint detection (§6). Go/no-go. | 1–2 wk |
| 2 | Deterministic job names + idempotent submit (K8s first, then grid by job name, Batch by name/tag). Persist attempt/failCount. | 2–3 wk |
| 3 | `attach(task, key, workDir, handle)` on `TaskResolver` (today only `resume`/`launch`); `reattach(handle)` on `TaskHandler`; a `TaskCacheStrategy` that does claim / attach / collect-finished / relaunch-as-infra-failure. Mind the retry hash chain `H(H(h,1),2)`. | 4–6 wk (K8s) |
| 4 | Detach-on-exit mode for `TaskPollingMonitor.cleanup()`; explicit cancel still kills. | 1–2 wk |
| 5 | K8s Run CRD, ownerReferences, lease + epoch fencing, drain/cancel. nf-k8s or separate operator. | 4–6 wk |
| 6 | Replay determinism: drift diagnostics, lint for order-dependent ops / time / random in closures (nf-lang), idempotent publishDir and observers. | ongoing |
| 7 | Measure replay cost on the largest real pipeline; optimise only if needed. | 2–4 wk |

Total, K8s only, one engineer incl. upstream review: roughly 5–7 months. Draft estimate.

## 8. Explicitly out of scope: headless engine / rewrite

Background research (same session) concluded:

- Codebase ≈ 200k LOC main (84% Groovy) + ≈ 180k LOC tests.
- Scripts and config compile to JVM bytecode even with the v2 strict-syntax parser; `lib/`,
  JVM plugins (PF4J), `new java.*`, and Groovy GDK methods mean nf-core compatibility needs a JVM.
- A rewrite (e.g. Rust) does not by itself remove the head: statelessness is architectural.
  Engines without a per-run driver (Argo, Flyte, Step Functions) restrict dynamism; Temporal
  allows it but pays with replay cost and history limits (~51k events).
- Comparable effort: Sprocket (Rust WDL engine) ~3 years and still pre-1.0.

Only revisit if hundreds of concurrent runs make one JVM per run a real cost.

## 9. Upstream strategy

- Land PR #6919 first; `orphaned` / `preempted` then become classified infra failures driven by
  `retryOn`.
- Open a separate GitHub issue/discussion + ADR (`adr/`) before coding core changes.
- PR sequence: deterministic names → `attach` primitive + K8s reattach → detach-on-exit →
  grid/Batch reattach. The Run CRD/operator lives in nf-k8s or out of tree.
- Coordinate with Seqera Platform's cache backup/restore (`plugins/nf-tower/.../CacheManager.groovy`).

## 10. Open questions

1. Can quiescence be detected cheaply in the GPars network? (Phase 1 spike)
2. Strict vs lenient orphan policy default?
3. Run CRD inside nf-k8s, or a standalone operator?
4. Should deterministic pod names break any existing tooling relying on the current pattern?
