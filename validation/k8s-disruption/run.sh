#!/usr/bin/env bash
#
# Reproduce Kubernetes pod disruptions and capture the pod/job status that
# Nextflow (nf-k8s) sees, to validate the infrastructure-failure classification.
#
# Generic by design: it uses the current kubectl context (or --context) and
# contains no cluster, project or credential details. Captured JSON is scrubbed
# (node names, IPs, UIDs, container/image IDs) so it can be used as test fixtures.
#
# Safety:
#   - only targets nodes selected by --node-label that ALSO carry the dedicated
#     NoSchedule taint --taint-key, and refuses if non-DaemonSet pods from other
#     namespaces run there
#   - all test objects live in their own namespace, plus two PriorityClasses
#   - every mutating step is printed and confirmed (unless --yes); --dry-run
#     prints them without executing anything
#   - a trap removes the temporary NoExecute taint; `cleanup` removes everything
#
# Usage: ./run.sh [options] <command> [scenario...]   (see ./run.sh --help)
#
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
CTX=""
NS=nf-k8s-disruption
LABEL=nf-k8s-disruption=true
TAINT_KEY=nf-k8s-disruption
MODES="pod job"
OUT="$PWD/captures"
IMAGE=busybox:1.36
E2E_IMAGE=ubuntu:24.04
DRY=false
YES=false
TIMEOUT=300
MAX_INFRA_RETRIES=2
KIND_NAME=nf-k8s-disruption
KIND_WORK="$HERE/.work"
LOW_PC=nf-k8s-disruption-low
HIGH_PC=nf-k8s-disruption-high
NOEXEC_TAINT=nf-k8s-disruption-noexecute
PART_OF=nf-k8s-disruption
NODE=""
SAFE_SCENARIOS="success exit-error oom ephemeral-limit api-eviction preemption taint"
# node scenarios take the node away: run them one at a time and restore the node in between
NODE_SCENARIOS="node-shutdown node-lost node-delete"
ALL_SCENARIOS="$SAFE_SCENARIOS $NODE_SCENARIOS"
E2E_SCENARIOS="api-eviction taint oom"

usage() {
cat <<EOF
Usage: $0 [options] <command> [scenario...]

Commands:
  kind-up           create a local kind cluster with a dedicated labelled/tainted worker
  kind-down         delete that kind cluster
  kind-all          on a fresh kind cluster: check, Nextflow e2e, then the node scenarios
                    (kind restores the worker itself); the log is saved to <out>/kind-all.log
  check             read-only: show the target node, guards and what would run
  capture [S...]    run scenarios with plain pods/jobs and capture scrubbed status JSON
                    (default: $SAFE_SCENARIOS;
                     $NODE_SCENARIOS must be named explicitly; outside kind
                     run them one per run, with --mode pod|job)
  nfcore [S...]     run the pipeline with the nf-core error strategy and no k8s retry setting,
                    disrupting only the first attempt: shows whether each disruption is
                    retried, and by which path, with the current build (kind only)
  e2e [S...]        run a Nextflow pipeline (dev build, ../../launch.sh) and disrupt its
                    task pod; kind only (needs a hostPath work dir shared with the host)
  cleanup           delete the namespace, PriorityClasses, PV and temporary taint

Scenarios (capture): $ALL_SCENARIOS
Scenarios (e2e):     $E2E_SCENARIOS
Scenarios (nfcore):  $NFCORE_SCENARIOS

Options:
  --context CTX       kubectl context (default: current context)
  --namespace NS      namespace for test objects (default: $NS)
  --node-label K=V    label selecting the dedicated node(s) (default: $LABEL)
  --taint-key KEY     NoSchedule taint key those nodes must carry (default: $TAINT_KEY)
  --mode pod|job|both K8s resource used to run the workload (default: both)
  --out DIR           output directory (default: ./captures)
  --image IMG         image for capture scenarios (default: $IMAGE)
  --timeout SEC       max capture time per scenario (default: $TIMEOUT)
  --dry-run           print mutating commands without running them
  --yes               do not ask for confirmation
EOF
}

log()  { printf '\033[1m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
warn() { printf '\033[33m[%s] WARN: %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf '\033[31m[%s] ERROR: %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

# read-only kubectl
kc() { if [[ -n $CTX ]]; then kubectl --context "$CTX" "$@"; else kubectl "$@"; fi; }
kn() { kc -n "$NS" "$@"; }

confirm() {
    $DRY && return 0
    $YES && return 0
    local ans
    read -r -p "$* [y/N] " ans </dev/tty
    [[ $ans == y || $ans == Y ]] || die "aborted by user"
}

# mutating command: print, skip on --dry-run
act() {
    log "+ $*"
    $DRY && return 0
    "$@"
}
kact() { if [[ -n $CTX ]]; then act kubectl --context "$CTX" "$@"; else act kubectl "$@"; fi; }

# apply a JSON manifest given on stdin
kapply() {
    local f; f=$(mktemp)
    cat > "$f"
    log "+ kubectl ${CTX:+--context $CTX }apply -f - <<< $(jq -c '{kind, name: .metadata.name}' "$f")"
    if ! $DRY; then kc apply -f "$f" >/dev/null; fi
    rm -f "$f"
}

require() { command -v "$1" >/dev/null || die "'$1' is required"; }

#
# target node selection and guards
#
select_node() {
    local nodes n
    nodes=$(kc get nodes -l "$LABEL" -o jsonpath='{.items[*].metadata.name}')
    [[ -n $nodes ]] || die "no node matches label '$LABEL'"
    for n in $nodes; do
        kc get node "$n" -o json | jq -e --arg k "$TAINT_KEY" \
            '[.spec.taints[]? | select(.key==$k and .effect=="NoSchedule")] | length > 0' >/dev/null \
            || die "a node matching '$LABEL' has no NoSchedule taint '$TAINT_KEY' -- refusing (not a dedicated node)"
    done
    NODE=${nodes%% *}
    kc get node "$NODE" -o json | jq -e '.status.conditions[] | select(.type=="Ready") | .status=="True"' >/dev/null \
        || die "target node is not Ready"
    local foreign
    foreign=$(foreign_pods)
    [[ -z $foreign ]] || die "non-DaemonSet pods from other namespaces run on the target node -- refusing:
$foreign"
    log "target node selected (label '$LABEL', taint '$TAINT_KEY'); name is not written to captures"
}

# running non-DaemonSet pods on the target node outside our namespace
foreign_pods() {
    kc get pods -A --field-selector "spec.nodeName=$NODE" -o json | jq -r --arg ns "$NS" '
        .items[]
        | select(.metadata.namespace != $ns)
        | select(.status.phase != "Succeeded" and .status.phase != "Failed")
        | select([.metadata.ownerReferences[]? | select(.kind=="DaemonSet")] | length == 0)
        | "  \(.metadata.namespace)/\(.metadata.name)"'
}

# DaemonSet pods on the node that a high-priority test pod could preempt
preemptable_pods() {
    kc get pods -A --field-selector "spec.nodeName=$NODE" -o json | jq -r --arg ns "$NS" '
        .items[]
        | select(.metadata.namespace != $ns)
        | select(.status.phase != "Succeeded" and .status.phase != "Failed")
        | select((.spec.priority // 0) < 1000000)
        | "  \(.metadata.namespace)/\(.metadata.name) (priority \(.spec.priority // 0))"'
}

to_milli() { local v=$1; if [[ $v == *m ]]; then echo "${v%m}"; else echo $(( v * 1000 )); fi; }

# free CPU on the target node in millicores (allocatable minus requests of running pods)
free_cpu_milli() {
    local alloc used
    alloc=$(to_milli "$(kc get node "$NODE" -o jsonpath='{.status.allocatable.cpu}')")
    used=$(kc get pods -A --field-selector "spec.nodeName=$NODE" -o json | jq '
        [.items[]
         | select(.status.phase != "Succeeded" and .status.phase != "Failed")
         | .spec.containers[].resources.requests.cpu // "0"
         | if endswith("m") then (.[:-1] | tonumber) else (tonumber * 1000) end] | add // 0 | floor')
    echo $(( alloc - used ))
}

#
# scrubbing: keep what the nf-k8s client reads, drop anything identifying
#
scrub() {
    jq -S --arg node "$NODE" '
        def red: if type == "string" and $node != "" then gsub($node; "node-1") else . end;
        {
          apiVersion, kind,
          metadata: (.metadata | {
              name,
              namespace: "default",
              labels: ((.labels // {}) | with_entries(select(.key | test("uid") | not))),
              deletionTimestamp, deletionGracePeriodSeconds
          } | with_entries(select(.value != null))),
          spec: (.spec | {nodeName, priority, priorityClassName, restartPolicy, backoffLimit, podFailurePolicy}
                 | with_entries(select(.value != null))
                 | if .nodeName then .nodeName = "node-1" else . end),
          status: ((.status // {}) | del(.hostIP, .hostIPs, .podIP, .podIPs))
        }
        | walk(if type == "object" then del(.uid, .containerID, .imageID, .resourceVersion, .managedFields) else . end)
        | walk(red)'
}

scrub_events() {
    jq -S --arg node "$NODE" '
        def red: if type == "string" and $node != "" then gsub($node; "node-1") else . end;
        [.items[] | {type, reason, message, source: (.source.component // .reportingComponent),
                     count, firstTimestamp, lastTimestamp, involvedObject: .involvedObject.kind}]
        | sort_by(.firstTimestamp) | walk(red)'
}

# one-line description of a scrubbed pod snapshot
describe_pod() {
    jq -r '[
        "phase=\(.status.phase // "-")",
        "reason=\(.status.reason // "-")",
        "deleting=\(.metadata.deletionTimestamp != null)",
        "conditions=\([.status.conditions[]? | select(.type=="DisruptionTarget") | "\(.type):\(.status)(\(.reason))"] | join(",") | if . == "" then "-" else . end)",
        "container=\([.status.containerStatuses[]?.state | to_entries[] | "\(.key)\(if .key=="terminated" then "(\(.value.exitCode),\(.value.reason))" else "" end)"] | join(",") | if . == "" then "-" else . end)"
    ] | join(" ")'
}

describe_job() {
    jq -r '"job active=\(.status.active // 0) failed=\(.status.failed // 0) succeeded=\(.status.succeeded // 0) conditions=\([.status.conditions[]? | "\(.type):\(.status)(\(.reason // "-"))"] | join(","))"'
}

#
# manifests
#
# pod_spec <command> <resources-json> [priorityClass] [volumes-json] [mounts-json] [image]
pod_spec() {
    local cmd=$1 res=$2 pc=${3:-} vols=${4:-[]} mounts=${5:-[]} img=${6:-$IMAGE}
    jq -n --arg cmd "$cmd" --argjson res "$res" --arg pc "$pc" --argjson vols "$vols" --argjson mounts "$mounts" \
          --arg img "$img" --arg lk "${LABEL%%=*}" --arg lv "${LABEL#*=}" --arg node "$NODE" --arg tk "$TAINT_KEY" '
        {
          restartPolicy: "Never",
          terminationGracePeriodSeconds: 10,
          nodeSelector: {($lk): $lv, "kubernetes.io/hostname": $node},
          tolerations: [{key: $tk, operator: "Exists", effect: "NoSchedule"}],
          volumes: $vols,
          containers: [{name: "main", image: $img, command: ["sh", "-c", $cmd], resources: $res, volumeMounts: $mounts}]
        } + (if $pc != "" then {priorityClassName: $pc} else {} end)'
}

# workload <mode> <name> <scenario> <pod-spec-json>
workload() {
    local mode=$1 name=$2 sc=$3 spec=$4
    jq -n --arg mode "$mode" --arg name "$name" --arg ns "$NS" --arg sc "$sc" --arg part "$PART_OF" --argjson spec "$spec" '
        {"app.kubernetes.io/part-of": $part, "nf-k8s-disruption/scenario": $sc} as $labels
        | if $mode == "pod" then
            {apiVersion: "v1", kind: "Pod", metadata: {name: $name, namespace: $ns, labels: $labels}, spec: $spec}
          else
            {apiVersion: "batch/v1", kind: "Job", metadata: {name: $name, namespace: $ns, labels: $labels},
             spec: {backoffLimit: 0, template: {metadata: {labels: $labels}, spec: $spec}}}
          end'
}

ensure_base() {
    kc get ns "$NS" >/dev/null 2>&1 || kact create namespace "$NS"
    kc get priorityclass "$LOW_PC" >/dev/null 2>&1 || jq -n --arg n "$LOW_PC" --arg part "$PART_OF" \
        '{apiVersion: "scheduling.k8s.io/v1", kind: "PriorityClass", metadata: {name: $n, labels: {"app.kubernetes.io/part-of": $part}},
          value: 1, globalDefault: false, preemptionPolicy: "Never", description: "nf-k8s disruption test victim"}' | kapply
    kc get priorityclass "$HIGH_PC" >/dev/null 2>&1 || jq -n --arg n "$HIGH_PC" --arg part "$PART_OF" \
        '{apiVersion: "scheduling.k8s.io/v1", kind: "PriorityClass", metadata: {name: $n, labels: {"app.kubernetes.io/part-of": $part}},
          value: 1000000, globalDefault: false, description: "nf-k8s disruption test preemptor"}' | kapply
}

# name of the (latest) pod for a workload
pod_of() {
    local mode=$1 name=$2
    if [[ $mode == pod ]]; then echo "$name"; return; fi
    kn get pods -l "job-name=$name" -o json 2>/dev/null \
        | jq -r '.items | sort_by(.metadata.creationTimestamp) | last | .metadata.name // empty'
}

wait_running() {
    local mode=$1 name=$2 i pod phase
    $DRY && return 0
    for i in $(seq 1 180); do
        pod=$(pod_of "$mode" "$name")
        if [[ -n $pod ]]; then
            phase=$(kn get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
            [[ $phase == Running ]] && return 0
            [[ $phase == Failed || $phase == Succeeded ]] && die "pod finished before it could be disrupted ($phase)"
        fi
        sleep 1
    done
    die "timed out waiting for the pod to run"
}

# save a scrubbed snapshot when it differs from the previous one
snapshot() {
    local dir=$1 kind=$2 json=$3 t=$4 s last n
    s=$(scrub <<<"$json")
    last="$dir/.last-$kind"
    if [[ -f $last ]] && [[ "$s" == "$(cat "$last")" ]]; then return 0; fi
    printf '%s\n' "$s" > "$last"
    n=$(ls "$dir" | grep -c "^$kind-" || true)
    printf '%s\n' "$s" > "$dir/$kind-$(printf '%02d' $((n + 1))).json"
    if [[ $kind == pod ]]; then
        echo "t+${t}s pod $(describe_pod <<<"$s")" | tee -a "$dir/timeline.log" >&2
    else
        echo "t+${t}s $(describe_job <<<"$s")" | tee -a "$dir/timeline.log" >&2
    fi
}

# poll pod (and job) status until the workload is finished and stable, or gone
capture_loop() {
    local mode=$1 name=$2 dir=$3 start=$SECONDS stable=0 pod json phase t jdone=false pgone=false lastpod=""
    $DRY && return 0
    while (( SECONDS - start < TIMEOUT )); do
        t=$(( SECONDS - start ))
        pod=$(pod_of "$mode" "$name"); [[ -n $pod ]] && lastpod=$pod
        if [[ $mode == job ]]; then
            if json=$(kn get job "$name" -o json 2>/dev/null); then
                snapshot "$dir" job "$json" "$t"
                jq -e '[.status.conditions[]? | select((.type=="Failed" or .type=="Complete") and .status=="True")] | length > 0' <<<"$json" >/dev/null && jdone=true
            fi
        fi
        if [[ -n $lastpod ]] && json=$(kn get pod "$lastpod" -o json 2>/dev/null); then
            snapshot "$dir" pod "$json" "$t"
            phase=$(jq -r '.status.phase' <<<"$json")
            if [[ $phase == Failed || $phase == Succeeded ]]; then stable=$((stable + 1)); else stable=0; fi
        elif [[ -n $lastpod ]] && ! $pgone; then
            pgone=true
            echo "t+${t}s pod gone (GET returns 404)" | tee -a "$dir/timeline.log" >&2
        fi
        # done: pod gone or finished for 15s, and (job mode) the job reached a final condition
        if { $pgone || (( stable >= 15 )); } && { [[ $mode == pod ]] || $jdone; }; then break; fi
        sleep 1
    done
    (( SECONDS - start >= TIMEOUT )) && warn "capture timed out after ${TIMEOUT}s"
    if [[ -n $lastpod ]]; then
        kn get events --field-selector "involvedObject.name=$lastpod" -o json 2>/dev/null | scrub_events > "$dir/events-pod.json" || true
    fi
    if [[ $mode == job ]]; then
        kn get events --field-selector "involvedObject.name=$name" -o json 2>/dev/null | scrub_events > "$dir/events-job.json" || true
    fi
    rm -f "$dir"/.last-*
}

#
# scenarios: each defines the workload and an optional trigger
#
NOLIMIT='{"requests":{"cpu":"100m","memory":"32Mi"},"limits":{"memory":"64Mi"}}'

scenario_spec() {
    local sc=$1
    case $sc in
        success)         pod_spec 'echo ok' "$NOLIMIT" ;;
        exit-error)      pod_spec 'echo boom >&2; exit 3' "$NOLIMIT" ;;
        oom)             pod_spec 'tail /dev/zero' "$NOLIMIT" ;;
        ephemeral-limit) pod_spec 'i=0; while [ $i -lt 50 ]; do dd if=/dev/zero of=/scratch/f$i bs=1M count=20 2>/dev/null; i=$((i+1)); sleep 1; done; sleep 600' \
                             "$NOLIMIT" "" '[{"name":"scratch","emptyDir":{"sizeLimit":"100Mi"}}]' '[{"name":"scratch","mountPath":"/scratch"}]' ;;
        preemption)      pod_spec 'sleep 600' "{\"requests\":{\"cpu\":\"${VICTIM_CPU}m\",\"memory\":\"32Mi\"},\"limits\":{\"memory\":\"64Mi\"}}" "$LOW_PC" ;;
        api-eviction|taint|node-shutdown|node-lost|node-delete)
                         pod_spec 'sleep 600' "$NOLIMIT" ;;
        *) die "unknown scenario: $sc" ;;
    esac
}

needs_running() { [[ $1 != success && $1 != exit-error && $1 != oom && $1 != ephemeral-limit ]]; }

trigger() {
    local sc=$1 mode=$2 name=$3 pod
    pod=$(pod_of "$mode" "$name")
    case $sc in
        api-eviction)
            log "+ kubectl create --raw /api/v1/namespaces/$NS/pods/$pod/eviction (Eviction API, as used by kubectl drain)"
            $DRY || jq -n --arg n "$pod" --arg ns "$NS" '{apiVersion: "policy/v1", kind: "Eviction", metadata: {name: $n, namespace: $ns}}' \
                | kc create --raw "/api/v1/namespaces/$NS/pods/$pod/eviction" -f - >/dev/null
            ;;
        preemption)
            jq -n --arg name "$name-preemptor" --arg ns "$NS" --arg part "$PART_OF" \
                  --argjson spec "$(pod_spec 'sleep 30' "{\"requests\":{\"cpu\":\"${VICTIM_CPU}m\",\"memory\":\"32Mi\"},\"limits\":{\"memory\":\"64Mi\"}}" "$HIGH_PC")" \
                '{apiVersion: "v1", kind: "Pod", metadata: {name: $name, namespace: $ns, labels: {"app.kubernetes.io/part-of": $part}}, spec: $spec}' | kapply
            ;;
        taint)
            TAINTED=true
            kact taint node "$NODE" "$NOEXEC_TAINT=true:NoExecute" >/dev/null
            ;;
        node-shutdown)
            if is_kind; then
                # announce the shutdown as systemd-logind does, as the Kubernetes node e2e tests do:
                # the kubelet terminates the pods (graceful node shutdown) while the node keeps running
                act docker exec "$NODE" dbus-send --system --type=signal /org/freedesktop/login1 \
                    org.freedesktop.login1.Manager.PrepareForShutdown boolean:true
                return 0
            fi
            cat >&2 <<EOF

  >>> Now shut down the target node VM from another terminal (an ACPI shutdown, not a
  >>> hard power off) so that the kubelet performs a graceful node shutdown.
  >>> Target node: $NODE
  >>>   GKE:  gcloud compute instances stop $NODE --zone <zone>
  >>> Status is being captured in the background; it stops after ${TIMEOUT}s.

EOF
            ;;
        node-lost)
            if is_kind; then
                # the kind node is a container without D-Bus: stopping it is not seen by the
                # kubelet, as when a VM disappears without notice
                act docker stop -t 60 "$NODE" >/dev/null
                return 0
            fi
            cat >&2 <<EOF

  >>> Now power off the target node VM abruptly from another terminal (e.g. a hard
  >>> reset), so that the kubelet cannot terminate the pods. Target node: $NODE
  >>> Status is being captured in the background; it stops after ${TIMEOUT}s.

EOF
            ;;
        node-delete)
            # the Node object disappears (as when a cloud provider removes a VM): the pod
            # garbage collector fails and deletes the pods bound to it (DeletionByPodGC)
            kact delete node "$NODE" --wait=false >/dev/null
            is_kind && return 0
            cat >&2 <<EOF

  >>> Node object deleted. Pod GC should fail and delete the pod within ~1 minute.
  >>> Afterwards restore the node so that the kubelet registers it again:
  >>>   kind: docker restart $NODE
  >>>   GKE:  recreate the VM (e.g. delete the instance and let the node pool replace it)
  >>> Check that it is back with its label and taint before running other scenarios.

EOF
            ;;
    esac
}

is_kind() {
    local ctx=${CTX:-$(kubectl config current-context 2>/dev/null || true)}
    [[ $ctx == kind-* ]]
}

# bring the kind worker back after a node scenario, with its dedicated label and taint
restore_kind_node() {
    local sc=$1 i
    case $sc in
        # cancel the shutdown: the kubelet marks the node as Ready again
        node-shutdown) act docker exec "$NODE" dbus-send --system --type=signal /org/freedesktop/login1 \
                           org.freedesktop.login1.Manager.PrepareForShutdown boolean:false ;;
        node-lost)     act docker start "$NODE" >/dev/null ;;
        *)             act docker restart "$NODE" >/dev/null ;;
    esac
    $DRY && return 0
    log "waiting for the node to be registered and Ready"
    for i in $(seq 1 90); do kc get node "$NODE" >/dev/null 2>&1 && break; sleep 2; done
    kc wait --for=condition=Ready "node/$NODE" --timeout=180s >/dev/null || die "the node did not become Ready"
    # a node registered again by the kubelet may miss the label and taint added at cluster creation
    kact label node "$NODE" "$LABEL" --overwrite >/dev/null
    kact taint node "$NODE" "$TAINT_KEY=true:NoSchedule" --overwrite >/dev/null
}

# the kind node image runs neither D-Bus nor systemd-logind, therefore the kubelet fails to start
# its node shutdown manager. Install and start them, then restart the kubelet
prepare_kind_shutdown() {
    if ! docker exec "$NODE" sh -c 'command -v dbus-send >/dev/null'; then
        act docker exec "$NODE" sh -c 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq dbus >/dev/null'
    fi
    act docker exec "$NODE" systemctl start dbus systemd-logind
    local since; since=$(docker exec "$NODE" date '+%Y-%m-%d %H:%M:%S')
    act docker exec "$NODE" systemctl restart kubelet
    $DRY && return 0
    sleep 10
    if docker exec "$NODE" journalctl -u kubelet --since "$since" --no-pager | grep -q "Failed to start node shutdown manager"; then
        die "the kubelet node shutdown manager failed to start: see 'docker exec $NODE journalctl -u kubelet'"
    fi
    kc wait --for=condition=Ready "node/$NODE" --timeout=120s >/dev/null || die "the node did not become Ready"
    log "kubelet node shutdown manager started"
}

untaint() {
    if [[ ${TAINTED:-false} == true && -n $NODE ]]; then
        kact taint node "$NODE" "$NOEXEC_TAINT-" >/dev/null || true
        TAINTED=false
    fi
}

run_capture() {
    local sc mode name dir free
    [[ $# -gt 0 ]] || set -- $SAFE_SCENARIOS
    select_node
    for sc in "$@"; do
        [[ " $ALL_SCENARIOS " == *" $sc "* ]] || die "unknown scenario: $sc"
        if [[ " $NODE_SCENARIOS " == *" $sc "* ]]; then
            if is_kind; then
                confirm "'$sc' stops or deletes the kind worker; it is restored after each run. Continue?"
            else
                [[ $# -eq 1 && $MODES != "pod job" ]] \
                    || die "'$sc' takes the node away: run it alone with --mode pod or --mode job, and restore the node in between"
                confirm "'$sc' makes the dedicated node unavailable until you restore it. Continue?"
            fi
        fi
        if [[ $sc == preemption ]]; then
            local pp; pp=$(preemptable_pods)
            [[ -z $pp ]] || die "pods with priority below the test preemptor run on the node; they could be preempted -- refusing:
$pp"
        fi
    done
    log "scenarios: $*  modes: $MODES  output: $OUT"
    confirm "Create namespace '$NS' + PriorityClasses and run these scenarios on the dedicated node?"
    ensure_base
    for sc in "$@"; do
        for mode in $MODES; do
            name="nf-disrupt-$sc-$mode-$(date +%s)"
            dir="$OUT/$sc/$mode"
            [[ $sc == node-shutdown ]] && is_kind && prepare_kind_shutdown
            if [[ $sc == preemption ]]; then
                free=$(free_cpu_milli)
                VICTIM_CPU=$(( free * 60 / 100 ))
                (( VICTIM_CPU >= 200 )) || die "not enough free CPU on the node for the preemption scenario (${free}m)"
            fi
            [[ $sc == taint ]] && confirm "The 'taint' scenario adds a NoExecute taint to the node: DaemonSet pods that do not tolerate it are evicted too. Continue?"
            log "=== scenario $sc ($mode) ==="
            $DRY || { rm -rf "$dir"; mkdir -p "$dir"; }
            workload "$mode" "$name" "$sc" "$(scenario_spec "$sc")" | kapply
            capture_loop "$mode" "$name" "$dir" &
            local cap=$!
            if needs_running "$sc"; then wait_running "$mode" "$name"; sleep 3; trigger "$sc" "$mode" "$name"; fi
            wait $cap || true
            untaint
            kact -n "$NS" delete "$([[ $mode == pod ]] && echo pod || echo job)" "$name" --ignore-not-found --wait=false >/dev/null
            [[ $sc == preemption ]] && kact -n "$NS" delete pod "$name-preemptor" --ignore-not-found --wait=false >/dev/null
            if [[ " $NODE_SCENARIOS " == *" $sc "* ]]; then
                if is_kind; then restore_kind_node "$sc"; else warn "restore the node before running further scenarios"; fi
            fi
        done
    done
    log "done: captures in $OUT (each scenario has timeline.log, pod-NN.json, job-NN.json, events-*.json)"
}

#
# Nextflow end-to-end (kind only)
#
run_e2e() {
    local sc mode dir cfg nxf pid i pod uid seen hits rc expected_hits reason task_exit expected_exit failed=0
    [[ $# -gt 0 ]] || set -- $E2E_SCENARIOS
    local ctx=${CTX:-$(kubectl config current-context)}
    [[ $ctx == kind-* ]] || die "e2e runs on kind only (the task work dir must be shared with the host); current context is not a kind context"
    nxf="$HERE/../../launch.sh"
    [[ -x $nxf ]] || die "Nextflow launcher not found at $nxf (run 'make compile' in the repo root first)"
    select_node
    confirm "Run the Nextflow e2e scenarios ($*) on the kind cluster?"
    ensure_base
    e2e_storage
    for sc in "$@"; do
        [[ " $E2E_SCENARIOS " == *" $sc "* ]] || die "unknown e2e scenario: $sc"
        expected_hits=$MAX_INFRA_RETRIES
        expected_exit=-
        case $sc in
            api-eviction) reason='disruption: EvictionByEvictionAPI' ;;
            taint) reason='disruption: DeletionByTaintManager' ;;
            oom) reason='reason: OOMKilled'; expected_hits=0; expected_exit=137 ;;
        esac
        for mode in $MODES; do
            dir="$OUT/e2e/$sc/$mode"
            log "=== e2e $sc ($mode) ==="
            $DRY && continue
            # pods of the previous run may still be terminating: wait for them to be gone so that
            # they cannot be mistaken for the pods of this run
            kn wait --for=delete pod -l nextflow.io/app=nextflow --timeout=180s >/dev/null 2>&1 || true
            rm -rf "$dir"; mkdir -p "$dir"
            cfg="$dir/nextflow.config"
            e2e_config "$ctx" "$mode" > "$cfg"
            ( cd "$dir" && NXF_ANSI_LOG=false "$nxf" -log "$dir/nextflow.log" -c "$cfg" run "$HERE/disruption.nf" \
                --scenario "$sc" -work-dir "$KIND_WORK/work" > "$dir/stdout.log" 2>&1 ) &
            pid=$!
            if [[ $sc != oom ]]; then
                # disrupt every attempt: MAX_INFRA_RETRIES retries, then errorStrategy decides
                seen=""
                for i in $(seq 1 $((MAX_INFRA_RETRIES + 1))); do
                    pod=""; uid=""
                    while kill -0 $pid 2>/dev/null; do
                        read -r pod uid < <(kn get pods -l nextflow.io/app=nextflow -o json | jq -r \
                            '.items[] | select(.status.phase=="Running" and .metadata.deletionTimestamp == null) | "\(.metadata.name) \(.metadata.uid)"' | head -1) || true
                        [[ -n $uid && " $seen " != *" $uid "* ]] && break
                        pod=""; sleep 1
                    done
                    [[ -n $pod ]] || break
                    seen="$seen $uid"; sleep 5
                    log "disrupting attempt $i"
                    trigger "$sc" pod "$pod"
                    sleep 5; untaint
                done
            fi
            wait $pid && rc=0 || rc=$?
            hits=$(grep -c 'Execution is retried' "$dir/nextflow.log" || true)
            task_exit=$(awk '/^Command exit status:$/ { getline; print $1; exit }' "$dir/stdout.log")
            {
                echo "scenario=$sc mode=$mode nextflow_exit=$rc infra_retries_logged=$hits (max $MAX_INFRA_RETRIES)"
                grep -E 'NOTE: .*Execution is retried|Max infrastructure retries|Caused by:|execution failed|exit status' "$dir/nextflow.log" "$dir/stdout.log" | sed "s/$NODE/node-1/g" || true
            } | tee "$dir/result.txt" >&2
            if [[ $rc != 1 || $hits != "$expected_hits" || $task_exit != "$expected_exit" ]] ||
                ! awk -v reason="$reason" '/^Caused by:$/ { getline; if (index($0, reason)) found=1 } END { exit !found }' "$dir/stdout.log"; then
                log "FAIL $sc ($mode): expected Nextflow exit 1, $expected_hits retries, task exit $expected_exit and final cause '$reason'; see $dir/result.txt"
                failed=1
            fi
        done
    done
    return "$failed"
}

#
# nf-core error strategy: are disrupted tasks retried by `errorStrategy` on exit code alone?
#
NFCORE_SCENARIOS="api-eviction preemption taint node-shutdown node-delete oom"
NFCORE_TIMEOUT=900

run_nfcore() {
    local sc mode dir cfg nxf pid pod uid rc start i hung path seen_exit seen_error subs outcome build summary
    [[ $# -gt 0 ]] || set -- $NFCORE_SCENARIOS
    local ctx=${CTX:-$(kubectl config current-context)}
    [[ $ctx == kind-* ]] || die "nfcore runs on kind only (the task work dir must be shared with the host)"
    nxf="$HERE/../../launch.sh"
    [[ -x $nxf ]] || die "Nextflow launcher not found at $nxf (run 'make compile' in the repo root first)"
    build=$(git -C "$HERE/../.." rev-parse --short HEAD)
    select_node
    for sc in "$@"; do [[ " $NFCORE_SCENARIOS " == *" $sc "* ]] || die "unknown nfcore scenario: $sc"; done
    if [[ " $* " == *" preemption "* ]]; then
        local pp; pp=$(preemptable_pods)
        [[ -z $pp ]] || die "pods with priority below the test preemptor run on the node -- refusing:
$pp"
    fi
    confirm "Run the nf-core errorStrategy scenarios ($*) with the Nextflow build $build on the kind cluster?"
    ensure_base
    e2e_storage
    summary="$OUT/nfcore/$build/summary.tsv"
    $DRY || { mkdir -p "$(dirname "$summary")"; printf 'scenario\tmode\toutcome\tretried_by\texit_seen\tsubmissions\tfirst_error\n' > "$summary"; }
    for sc in "$@"; do
        for mode in $MODES; do
            dir="$OUT/nfcore/$build/$sc/$mode"
            log "=== nfcore $sc ($mode), build $build ==="
            $DRY && continue
            kn wait --for=delete pod -l nextflow.io/app=nextflow --timeout=180s >/dev/null 2>&1 || true
            rm -rf "$dir"; mkdir -p "$dir"
            cfg="$dir/nextflow.config"
            if [[ $sc == preemption ]]; then
                VICTIM_CPU=$(( $(free_cpu_milli) * 60 / 100 / 1000 * 1000 ))
                (( VICTIM_CPU >= 1000 )) || die "not enough free CPU on the node for the preemption scenario"
            fi
            [[ $sc == node-shutdown ]] && prepare_kind_shutdown
            nfcore_config "$ctx" "$mode" "$sc" > "$cfg"
            ( cd "$dir" && NXF_ANSI_LOG=false "$nxf" -log "$dir/nextflow.log" -c "$cfg" run "$HERE/disruption.nf" \
                --scenario "$sc" --sleep 45 -work-dir "$KIND_WORK/work" > "$dir/stdout.log" 2>&1 ) &
            pid=$!
            start=$SECONDS; hung=false
            if [[ $sc != oom ]]; then
                # disrupt only the first attempt: a retry, by any means, then completes
                pod=""; uid=""
                while kill -0 $pid 2>/dev/null; do
                    read -r pod uid < <(kn get pods -l nextflow.io/app=nextflow -o json | jq -r \
                        '.items[] | select(.status.phase=="Running" and .metadata.deletionTimestamp == null) | "\(.metadata.name) \(.metadata.uid)"' | head -1) || true
                    [[ -n $uid ]] && break
                    sleep 1
                done
                if [[ -n $pod ]]; then
                    sleep 5
                    log "disrupting the first attempt"
                    trigger "$sc" pod "$pod"
                    sleep 5; untaint
                    if [[ " $NODE_SCENARIOS " == *" $sc "* ]]; then
                        # wait until the disrupted pod is resolved, then bring the node back so a retry can run
                        for i in $(seq 1 420); do
                            local ph; ph=$(kn get pod "$pod" -o jsonpath='{.metadata.uid} {.status.phase}' 2>/dev/null || true)
                            [[ -z $ph || $ph != "$uid "* || $ph == *Failed || $ph == *Succeeded ]] && break
                            sleep 1
                        done
                        restore_kind_node "$sc"
                    fi
                fi
            fi
            while kill -0 $pid 2>/dev/null; do
                if (( SECONDS - start > NFCORE_TIMEOUT )); then hung=true; kill $pid 2>/dev/null || true; break; fi
                sleep 2
            done
            wait $pid && rc=0 || rc=$?
            [[ $sc == preemption ]] && kact -n "$NS" delete pod -l app.kubernetes.io/part-of=$PART_OF --ignore-not-found --wait=false >/dev/null
            $hung && kact -n "$NS" delete pods,jobs -l nextflow.io/app=nextflow --ignore-not-found --wait=false >/dev/null
            # how was the disrupted attempt handled?
            path=none
            grep -qE 'NOTE: .*-- Execution is retried \([0-9]+\)' "$dir/nextflow.log" && path=errorStrategy
            grep -qE 'NOTE: .*-- Execution is retried$' "$dir/nextflow.log" && path=automatic
            seen_exit=$(grep -m1 'Task completed > TaskHandler' "$dir/nextflow.log" | sed -nE 's/.*exit: ([^;]*);.*/\1/p')
            seen_error=$(grep -m1 'Task completed > TaskHandler' "$dir/nextflow.log" | sed -nE 's/.*error: ([^;]*);.*/\1/p' | cut -c1-160)
            subs=$(grep -c 'Submitted process >' "$dir/nextflow.log" || true)
            if $hung; then outcome="hung (killed after ${NFCORE_TIMEOUT}s)"; elif [[ $rc == 0 ]]; then outcome=recovered; else outcome="failed (exit $rc)"; fi
            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$sc" "$mode" "$outcome" "$path" "${seen_exit:--}" "$subs" "${seen_error:--}" \
                | sed "s/$NODE/node-1/g" | tee -a "$summary" >&2
        done
    done
    $DRY || { log "summary: $summary"; { column -t -s $'\t' "$summary" 2>/dev/null || cat "$summary"; } >&2; }
}

nfcore_config() {
    local ctx=$1 mode=$2 sc=$3 extra=""
    [[ $sc == preemption ]] && extra=", [priorityClassName: '$LOW_PC']"
    cat <<EOF
process {
    executor = 'k8s'
    container = '$E2E_IMAGE'
    // error strategy of the nf-core pipeline template
    errorStrategy = { task.exitStatus in ((130..145) + 104 + 175) ? 'retry' : 'finish' }
    maxRetries = 1
    pod = [ [nodeSelector: '$LABEL'], [toleration: [key: '$TAINT_KEY', operator: 'Exists', effect: 'NoSchedule']]$extra ]
EOF
    [[ $sc == preemption ]] && echo "    cpus = $(( VICTIM_CPU / 1000 ))"
    cat <<EOF
}
k8s {
    context = '$ctx'
    namespace = '$NS'
    storageClaimName = 'nf-work'
    storageMountPath = '$KIND_WORK'
    computeResourceType = '$([[ $mode == pod ]] && echo Pod || echo Job)'
}
EOF
}

e2e_storage() {
    jq -n --arg path "$KIND_WORK" --arg ns "$NS" --arg part "$PART_OF" '
        {apiVersion: "v1", kind: "PersistentVolume", metadata: {name: "nf-k8s-disruption-work", labels: {"app.kubernetes.io/part-of": $part}},
         spec: {capacity: {storage: "5Gi"}, accessModes: ["ReadWriteOnce"], storageClassName: "", persistentVolumeReclaimPolicy: "Retain",
                hostPath: {path: $path}, claimRef: {namespace: $ns, name: "nf-work"}}}' | kapply
    jq -n --arg ns "$NS" '
        {apiVersion: "v1", kind: "PersistentVolumeClaim", metadata: {name: "nf-work", namespace: $ns},
         spec: {accessModes: ["ReadWriteOnce"], storageClassName: "", volumeName: "nf-k8s-disruption-work", resources: {requests: {storage: "5Gi"}}}}' | kapply
}

e2e_config() {
    local ctx=$1 mode=$2
    cat <<EOF
process {
    executor = 'k8s'
    container = '$E2E_IMAGE'
    errorStrategy = 'terminate'
    pod = [ [nodeSelector: '$LABEL'], [toleration: [key: '$TAINT_KEY', operator: 'Exists', effect: 'NoSchedule']] ]
}
k8s {
    context = '$ctx'
    namespace = '$NS'
    storageClaimName = 'nf-work'
    storageMountPath = '$KIND_WORK'
    computeResourceType = '$([[ $mode == pod ]] && echo Pod || echo Job)'
    maxInfraRetries = $MAX_INFRA_RETRIES
}
EOF
}

#
# kind
#
kind_up() {
    require kind
    local cfg; cfg=$(mktemp)
    cat > "$cfg" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
kubeadmConfigPatches:
- |
  kind: KubeletConfiguration
  shutdownGracePeriod: 30s
  shutdownGracePeriodCriticalPods: 10s
nodes:
- role: control-plane
- role: worker
  labels:
    ${LABEL%%=*}: "${LABEL#*=}"
  extraMounts:
  - hostPath: $KIND_WORK
    containerPath: $KIND_WORK
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      taints:
      - key: $TAINT_KEY
        value: "true"
        effect: NoSchedule
EOF
    log "kind config:"; sed 's/^/    /' "$cfg" >&2
    confirm "Create local kind cluster '$KIND_NAME'?"
    act mkdir -p "$KIND_WORK"
    act kind create cluster --name "$KIND_NAME" --config "$cfg"
    rm -f "$cfg"
    # the worker joins before its network is ready: wait for all nodes to be Ready
    if ! $DRY; then
        log "waiting for the nodes to be Ready"
        kubectl --context "kind-$KIND_NAME" wait --for=condition=Ready nodes --all --timeout=180s >/dev/null \
            || die "the kind nodes did not become Ready"
    fi
}

kind_down() {
    require kind
    confirm "Delete local kind cluster '$KIND_NAME'?"
    act kind delete cluster --name "$KIND_NAME"
}

kind_all() {
    require kind
    local ctx="kind-$KIND_NAME" opts="--yes --out $OUT" log="$OUT/kind-all.log"
    $DRY && opts="$opts --dry-run"
    confirm "Recreate the local kind cluster '$KIND_NAME' and run the e2e and node scenarios (about 20 minutes)?"
    mkdir -p "$OUT"
    step() { printf '\n\033[1m===== %s =====\033[0m\n' "$*"; }
    {
        step "1. Fresh kind cluster"
        if kind get clusters 2>/dev/null | grep -qx "$KIND_NAME"; then bash "$HERE/run.sh" $opts kind-down; fi
        bash "$HERE/run.sh" $opts kind-up
        step "2. Check"
        bash "$HERE/run.sh" $opts --context "$ctx" check
        step "3. Build the Nextflow development version"
        if $DRY; then log "+ make compile"; else (cd "$HERE/../.." && make compile); fi
        step "4. Nextflow end-to-end"
        bash "$HERE/run.sh" $opts --context "$ctx" e2e
        step "5. Node shutdown, node lost and node delete"
        bash "$HERE/run.sh" $opts --context "$ctx" --timeout 420 capture node-shutdown node-lost node-delete
        step "Done"
        log "e2e results: $OUT/e2e/*/*/result.txt"
        log "captures:    $OUT/node-shutdown $OUT/node-delete"
        log "tear down:   bash run.sh --context $ctx cleanup && bash run.sh kind-down"
    } 2>&1 | tee "$log"
}

cleanup() {
    confirm "Delete namespace '$NS', PriorityClasses '$LOW_PC'/'$HIGH_PC', the e2e PV and the '$NOEXEC_TAINT' taint?"
    local n
    for n in $(kc get nodes -l "$LABEL" -o jsonpath='{.items[*].metadata.name}'); do
        if kc get node "$n" -o json | jq -e --arg k "$NOEXEC_TAINT" '[.spec.taints[]? | select(.key==$k)] | length > 0' >/dev/null; then
            kact taint node "$n" "$NOEXEC_TAINT-"
        fi
    done
    kact delete namespace "$NS" --ignore-not-found
    kact delete priorityclass "$LOW_PC" "$HIGH_PC" --ignore-not-found
    kact delete pv nf-k8s-disruption-work --ignore-not-found
}

check() {
    select_node
    local free; free=$(free_cpu_milli)
    log "free CPU on target node: ${free}m (preemption victim would request $(( free * 60 / 100 ))m)"
    local pp; pp=$(preemptable_pods)
    if [[ -n $pp ]]; then warn "preemption scenario would be refused, low-priority pods on node:
$pp"; else log "preemption guard: OK"; fi
    local v
    for v in "create namespaces" "create priorityclasses.scheduling.k8s.io" "patch nodes" "create pods" "create jobs.batch -n $NS" "create pods/eviction -n $NS"; do
        log "can-i $v: $(kc auth can-i $v 2>/dev/null || true)"
    done
    log "server version: $(kc version -o json 2>/dev/null | jq -r '.serverVersion.gitVersion')"
}

#
# main
#
TAINTED=false
trap untaint EXIT
trap 'exit 130' INT TERM

CMD=""; ARGS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --context)    CTX=$2; shift 2 ;;
        --namespace)  NS=$2; shift 2 ;;
        --node-label) LABEL=$2; shift 2 ;;
        --taint-key)  TAINT_KEY=$2; shift 2 ;;
        --mode)       case $2 in pod) MODES=pod;; job) MODES=job;; both) MODES="pod job";; *) die "bad --mode";; esac; shift 2 ;;
        --out)        OUT=$(mkdir -p "$2" && cd "$2" && pwd); shift 2 ;;
        --image)      IMAGE=$2; shift 2 ;;
        --timeout)    TIMEOUT=$2; shift 2 ;;
        --dry-run)    DRY=true; shift ;;
        --yes)        YES=true; shift ;;
        -h|--help)    usage; exit 0 ;;
        -*)           die "unknown option $1" ;;
        *)            if [[ -z $CMD ]]; then CMD=$1; else ARGS="$ARGS $1"; fi; shift ;;
    esac
done

require kubectl
require jq
$DRY && log "DRY RUN: no changes will be made"

case $CMD in
    kind-up)   kind_up ;;
    kind-down) kind_down ;;
    kind-all)  kind_all ;;
    check)     check ;;
    capture)   run_capture $ARGS ;;
    e2e)       run_e2e $ARGS ;;
    nfcore)    run_nfcore $ARGS ;;
    cleanup)   cleanup ;;
    *)         usage; exit 1 ;;
esac
