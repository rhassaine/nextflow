# K8s pod disruption validation

Reproduces the pod disruptions that nf-k8s classifies as infrastructure failures
and captures the pod/job status the plugin actually sees, so the classification in
`K8sClient.checkInfraFailure()` can be checked against real clusters rather than
documentation examples.

`run.sh` only uses `kubectl` and `jq` against the current (or `--context`) context.
It contains no cluster, project or credential details, and the captured JSON is
scrubbed (node names, IPs, UIDs, container/image IDs), so it can be shared and used as
test fixtures.

## Safety

- The target node must match `--node-label` **and** carry the NoSchedule taint
  `--taint-key`. The script refuses to run if pods from other namespaces (other than
  DaemonSets) run on it.
- `preemption` refuses to run if any pod on the node has a priority below the test
  preemptor.
- Test objects: the namespace `nf-k8s-disruption`, the PriorityClasses
  `nf-k8s-disruption-low` (1) and `nf-k8s-disruption-high` (1000000), and the e2e PV.
- Every change is printed and confirmed. `--dry-run` executes nothing that changes the
  cluster, and `--yes` skips the prompts.
- `taint` adds a temporary NoExecute taint to the node. It is removed on exit, and
  also by `cleanup`.

## Scenarios

| Scenario | How | Expected nf-k8s classification |
|---|---|---|
| `success` | `exit 0` | none (baseline) |
| `exit-error` | `exit 3` | task failure, exit 3 (baseline: no infra retry) |
| `oom` | `tail /dev/zero` with a 64Mi limit | task failure, `reason: OOMKilled` in the error, exit 137 |
| `ephemeral-limit` | Writes past an `emptyDir.sizeLimit` → kubelet eviction | infra (`Evicted`, `TerminationByKubelet`) |
| `api-eviction` | Eviction API (what `kubectl drain` uses) | infra (`EvictionByEvictionAPI`, or pod gone) |
| `preemption` | A high-priority pod needs the victim's CPU | infra (`PreemptionByScheduler`, or pod gone) |
| `taint` | NoExecute taint on the node | infra (`DeletionByTaintManager`, or pod gone) |
| `node-shutdown` | Graceful node shutdown: the kubelet terminates the pods | infra (`TerminationByKubelet`) |
| `node-lost` | The node disappears without notice; the pod is evicted ~5 minutes later | infra (`DeletionByTaintManager`) |
| `node-delete` | Node object deleted, as when the cloud provider removes a VM | infra (`DeletionByPodGC`, or pod gone) |

The node scenarios take the node away. On kind, the script triggers them and restores
the worker itself:

- `node-shutdown`: the kind node image runs neither D-Bus nor systemd-logind, so the
  kubelet can't start its node shutdown manager. The script installs and starts them in
  the worker, restarts the kubelet, then sends logind's `PrepareForShutdown` signal, as
  the Kubernetes node e2e tests do.
- `node-lost`: stops the worker container.
- `node-delete`: deletes the Node object.

On other clusters, run each one on its own with `--mode pod` or `--mode job`, trigger it
when asked, and restore the node before the next run.

Each scenario runs as a bare Pod and as a Job with `backoffLimit: 0`, matching the two
`k8s.computeResourceType` values. For each one, `captures/<scenario>/<mode>/` holds:

- `timeline.log`: one line per status change, through to the pod disappearing;
- `pod-NN.json` and `job-NN.json`: every distinct scrubbed status;
- `events-*.json`: the matching events.

`e2e` runs `disruption.nf` with the development build (`launch.sh`) on kind and disrupts
every attempt. It checks that the task is retried `k8s.maxInfraRetries` times, then fails
under `errorStrategy` with the pod reason in the error message, and that `oom` is
reported as a task failure with `reason: OOMKilled`. It needs a work directory shared
between the host and the pods, which is why it is limited to kind.

## Reproduce

Requirements: `kubectl`, `jq`, and for the local cluster [kind](https://kind.sigs.k8s.io)
and Docker.

### Local (kind)

Everything in one go, on a fresh cluster (about 20 minutes):

```bash
cd validation/k8s-disruption
bash run.sh kind-all
bash run.sh kind-down
```

Or step by step:

```bash
cd validation/k8s-disruption
bash run.sh kind-up
bash run.sh --context kind-nf-k8s-disruption check
bash run.sh --context kind-nf-k8s-disruption capture
bash run.sh --context kind-nf-k8s-disruption capture node-shutdown node-lost node-delete
(cd ../.. && make compile)
bash run.sh --context kind-nf-k8s-disruption e2e
bash run.sh --context kind-nf-k8s-disruption cleanup
bash run.sh kind-down
```

On kind, the node scenarios stop, delete and restore the worker container themselves.

### Managed cluster

Use a dedicated, tainted node pool, so no other workload can be scheduled on it. For
example, on GKE:

```bash
gcloud container node-pools create nf-k8s-disruption \
  --cluster <cluster> --location <location> \
  --machine-type e2-standard-4 --num-nodes 1 --spot \
  --node-labels nf-k8s-disruption=true \
  --node-taints nf-k8s-disruption=true:NoSchedule

bash run.sh --context <context> check
bash run.sh --context <context> capture
bash run.sh --context <context> capture node-shutdown --mode pod   # stop the node VM when asked
bash run.sh --context <context> cleanup

gcloud container node-pools delete nf-k8s-disruption --cluster <cluster> --location <location>
```

Use `--dry-run` with any command to see what it would do.
