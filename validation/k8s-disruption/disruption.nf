/*
 * Task used by `run.sh e2e` and `run.sh nfcore`: the script disrupts its pod while it sleeps.
 * For the `oom` scenario the first attempt runs out of memory, the next ones succeed
 */
params.scenario = 'api-eviction'
params.sleep = 120

process DISRUPT_ME {
    memory { params.scenario == 'oom' && task.attempt == 1 ? '64 MB' : '128 MB' }

    script:
    if( params.scenario == 'oom' && task.attempt == 1 )
        'tail /dev/zero'
    else
        "echo 'attempt ${task.attempt}'; sleep ${params.sleep}"
}

workflow {
    DISRUPT_ME()
}
