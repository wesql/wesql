#!/usr/bin/env bash

set +e
# exported variables: NAMESPACE, THREADS, TABLES, SIZE
export THREADS="10"
export TABLES="1"
export SIZE="10"
declare -g SVC_NAME_PODS
declare -a NAME_PODS

DEFAULT_VALUE=0
DEFAULT_NAMESPACE="default"
DEFAULT_CLUSTER_NAME="wesql-cluster"
DEFAULT_PROVIDER="aws"
DEFAULT_REGION="cn-northwest-1"
DEFAULT_BUCKET="wesql-chaos-test"
DEFAULT_ENDPOINT=""
DEFAULT_USE_HTTPS="false"
DEFAULT_IMAGE="apecloud/wesql-server:8.0.35-0.1.0_beta3.38"
DEFAULT_AK=$(echo -n "${AWS_ACCESS_KEY_ID}" | base64)
DEFAULT_SK=$(echo -n "${AWS_SECRET_ACCESS_KEY}" | base64)

runningToStopTime="null"
stopToRunningTime="null"
podrunningToStopTime="null"
podStopToRunningTime="null"
test_fault_name=""
fullRecoveryTime="null"
injectFaultTime="null"
podeverstopped="false"

exit_program() {
    cmd='kubectl delete validatingwebhookconfigurations.admissionregistration.k8s.io chaos-mesh-validation-auth'
    eval_cmd "$cmd"
    cmd="kubectl delete IOChaos \$(kubectl get IOChaos |awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete PodChaos \$(kubectl get PodChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete NetworkChaos \$(kubectl get NetworkChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete StressChaos \$(kubectl get StressChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete AWSChaos \$(kubectl get AWSChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete GCPChaos \$(kubectl get GCPChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete pod \$(kubectl get pod | grep "sysbench-tpcc-" | awk '{print \$1}')"
    eval_cmd "$cmd"

    eval "rm -rf tmp"
    echo "Exiting..."
    report_test_result
    exit 0
}

trap exit_program SIGINT

eval_cmd() {
    eval_cmd="$1"
    echo -e "\033[1;30m
      $(tput -T xterm setaf 7)\`$eval_cmd\`$(tput -T xterm sgr0)
    \033[0m"
    eval "$eval_cmd"
}

show_help() {
    cat <<EOF
Usage: $(basename "$0") <options>
    -h, --help                  Display this help message and exit
    -n, --namespace             Specify the Kubernetes namespace (default: $DEFAULT_NAMESPACE)
    -cn, --cluster-name         Specify the Kubernetes cluster name (default: $DEFAULT_CLUSTER_NAME-)
    -op, --objstore-provider    Specify the object storage provider (e.g., AWS, MINIO, R2) (default: $DEFAULT_PROVIDER)
    -or, --objstore-region      Specify the object storage region (default: $DEFAULT_REGION)
    -ob, --objstore-bucket      Specify the object storage bucket (default: $DEFAULT_BUCKET)
    -oe, --objstore-endpoint    Specify the object storage endpoint (default: $DEFAULT_ENDPOINT)
    -ouh, --objstore-use-https  Specify whether to use HTTPS for the object storage (default: $DEFAULT_USE_HTTPS)
    -wi, --wesql-image          Specify the Wesql image to use (default: $DEFAULT_IMAGE)
    -ak64, --access-key-base64  Specify the Base64-encoded access key for the object storage (default: the environment variable AWS_ACCESS_KEY_ID)
    -sk64, --secret-key-base64  Specify the Base64-encoded secret key for the object storage (default: the environment variable AWS_SECRET_ACCESS_KEY)
    -th, --threads              Specify the number of threads used for tpcc test (default: $TABLES)
    -tb, --tables               Specify the tables to process used for tpcc test (default: $THREADS)
    -sz, -sc, --size, --scale   Specify the size or scale factor used for tpcc test (default: $SIZE)
    -d, --duration              Specify the duration (default: $DEFAULT_PROVIDER)
EOF
}

get_datapath() {
    datapath="/data/mysql"
    echo $datapath
}

pod_exec_command() {
    cmd=$1
    pod_idx=$2
    pod_name=${NAME_PODS[$pod_idx]}
    if ! res=$(kubectl exec -it ${pod_name} -c mysql -- sh -c "mysql -uroot -p1234 -s -N -e\"$cmd\" 2>/dev/null" | tr -d '\r'); then
        echo "Exec cmd $cmd failed"
        return 1
    fi
    echo "$res"
    return 0
}

leader_exec_command() {
    cmd=$1
    leader_pod_idx=$(get_leader_pod_idx)
    pod_name=${NAME_PODS[$leader_pod_idx]}
    if ! res=$(kubectl exec -it ${pod_name} -c mysql -- sh -c "mysql -uroot -p1234 -s -N -e\"$cmd\" 2>/dev/null" | tr -d '\r'); then
        echo "Exec cmd $cmd failed"
        return 1
    fi
    echo "$res"
    return 0
}

check_cluster_connect() {
    cmd="select 1"
    while true; do
        ready_num=0
        for i in {0..2}; do
            if res=$(pod_exec_command "$cmd" "$i"); then
              if [[ "$res" == "1" ]]; then
                ready_num=$(($ready_num + 1))
              fi
            fi
        done
        if [[ $ready_num -eq 3 ]]; then
            echo "check cluster connect done"
            break
        fi
        echo "check cluster connect ready num:$ready_num"
        sleep 5
    done
}

check_cluster_status_create() {
    while true; do
        pod0_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[0]}" || true) | (grep "1/1" || true) | awk '{print $3}')
        pod1_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[1]}" || true) | (grep "1/1" || true) | awk '{print $3}')
        pod2_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[2]}" || true) | (grep "1/1" || true) | awk '{print $3}')
        echo "pod0_status: $pod0_status, pod1_status: $pod1_status, pod2_status: $pod2_status"
        if [[ "$pod0_status" == "Running" && "$pod1_status" == "Running" && "$pod2_status" == "Running" ]]; then
            echo "check cluster create status done"
            break
        fi
        sleep 1
    done
    check_cluster_connect
}

create_cluster() {
    cmd="envsubst < serverless/serverless-cluster.yaml | kubectl apply -f -"
    eval_cmd "$cmd"
    check_cluster_status_create
}

check_cluster_status() {
    echo "check cluster status"
    cmd=""
    # reuse get_leader_pod_idx logic
    get_leader_pod_idx
    check_pod_status
    echo
    fullRecoveryTime=$(date +'%Y-%m-%d %H:%M:%S')
    if [[ "$uname" == "Darwin" ]]; then
        fullrecov=$(($(date -j -f "%Y-%m-%d %H:%M:%S" "$fullRecoveryTime" +%s) - $(date -j -f "%Y-%m-%d %H:%M:%S" "$runningToStopTime" +%s)))
    else
        fullrecov=$(($(date -d "$fullRecoveryTime" +%s) - $(date -d "$runningToStopTime" +%s)))
    fi
    echo "fullRecoveryTime is: $fullrecov"
    everstopped=0
    injectFaultTime="null"
    WAIT_TIME=0
}

check_pod_status() {
    while true; do
        echo "check pod status"
        pod0_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[0]}" || true) | (grep "1/1" || true) | awk '{print $3}')
        pod1_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[1]}" || true) | (grep "1/1" || true) | awk '{print $3}')
        pod2_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[2]}" || true) | (grep "1/1" || true) | awk '{print $3}')
        echo "pod0_status: $pod0_status, pod1_status: $pod1_status, pod2_status: $pod2_status"
        if [[ "$pod0_status" == "Running" && "$pod1_status" == "Running" && "$pod2_status" == "Running" ]]; then
            echo "$(tput -T xterm setaf 2)check pod status done$(tput -T xterm sgr0)"
            break
        fi
        sleep 1
    done
}

check_pod_running() {
    echo "check pod running"
    pod0_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[0]}" || true) | (grep "1/1" || true) | awk '{print $3}')
    pod1_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[1]}" || true) | (grep "1/1" || true) | awk '{print $3}')
    pod2_status=$(kubectl get pod --namespace $NAMESPACE | (grep "${NAME_PODS[2]}" || true) | (grep "1/1" || true) | awk '{print $3}')
    echo "pod0_status: $pod0_status, pod1_status: $pod1_status, pod2_status: $pod2_status"

    if [[ "$pod0_status" != "Running" || "$pod1_status" != "Running" || "$pod2_status" != "Running" ]]; then
        if [[ "$podeverstopped" == "false" ]]; then
            podeverstopped="true"
            podrunningToStopTime=$(date +'%Y-%m-%d %H:%M:%S')
            echo "podrunningToStopTime: "$podrunningToStopTime
        fi
    elif [[ "$podStopToRunningTime" == "null" && "$podeverstopped" == "true" ]]; then
        podStopToRunningTime=$(date +'%Y-%m-%d %H:%M:%S')
    fi
}

# cannot add other echo
get_leader_pod_idx() {
    cmd="select role='Leader' and SERVER_READY_FOR_RW='Yes' from information_schema.wesql_cluster_local;"
    cnt=900
    while [[ $cnt -ne 0 ]]; do
        for i in {0..2}; do
            if res=$(pod_exec_command "$cmd" "$i"); then
              if [[ "$res" == "1" ]]; then
                if [[ "$i" != "0" ]]; then
                    echo "Leader pod is not 0 pod!"
                    exit
                fi
                echo "$i"
                return
              fi
            fi
        done
        sleep 1
        cnt=$(($cnt - 1))
    done
}

get_follower1_pod_idx() {
    # cmd="select role='Follower' from information_schema.wesql_cluster_local;"
    # while true; do
    #     for i in {0..2}; do
    #         if ! res=$(pod_exec_command "$cmd" "$i"); then
    #             break
    #         fi
    #         if [[ "$res" == "1" ]]; then
    #             echo "$i"
    #             return
    #         fi
    #     done
    # done
    echo "1"
}

get_follower2_pod_idx() {
    # cmd="select role='Follower' from information_schema.wesql_cluster_local;"
    # while true; do
    #     for i in {2..0}; do
    #         if ! res=$(pod_exec_command "$cmd" "$i"); then
    #             break
    #         fi
    #         if [[ "$res" == "1" ]]; then
    #             echo "$i"
    #             return
    #         fi
    #     done
    # done
    echo "2"
}

tpcc_check() {
    # for i in {0..2}; do
    i=0
    export HOST=${NAME_PODS[$i]}.${SVC_NAME_PODS}
    export DB_USERNAME="root"
    export DB_PASSWORD="1234"
    check_cmd="envsubst < serverless/tpcc_check.yaml | kubectl create -f -"
    check_output=$(eval "$check_cmd")
    echo "$check_output"
    pod_name=$(echo "$check_output" | awk -F'[/ ]' '{print $(NF-1)}')
    time_cnt=1
    while true; do
        check_pod_status=$(kubectl get pod | (grep "$pod_name" || true) | awk '{print $3}')
        if [[ "$check_pod_status" == "Completed" ]]; then
            delete_tpcc_check_cmd="kubectl delete pod $pod_name"
            eval "$delete_tpcc_check_cmd"
            break
        elif [[ "$check_pod_status" == "Running" || "$check_pod_status" == "ContainerCreating" ]]; then
            sleep 1
        elif [[ "$check_pod_status" == "Error" ]]; then
            echo "Data inconsistent, tpcc check failed!!"
            return 1
        else
            if [[ "$time_cnt" -gt 20 ]]; then
                echo "Tpcc check failed!!"
                return 1
            else
                time_cnt=$(($time_cnt + 1))
                sleep 1
            fi
        fi
    done
    return 0
}

tpcc_run() {
    leader_pod_idx=$(get_leader_pod_idx)
    export HOST=${NAME_PODS[$leader_pod_idx]}.${SVC_NAME_PODS}
    export DB_USERNAME="root"
    export DB_PASSWORD="1234"
    run_cmd="envsubst < serverless/tpcc_run.yaml | kubectl create -f -"
    run_output=$(eval "$run_cmd")
    pod_name=$(echo "$run_output" | awk -F'[/ ]' '{print $(NF-1)}')
    while true; do
        run_pod_status=$(kubectl get pod | (grep "$pod_name" || true) | awk '{print $3}')
        if [[ "$run_pod_status" == "ContainerCreating" ]]; then
            sleep 1
        elif [[ "$run_pod_status" == "Running" ]]; then
            break
        else
            echo "Tpcc run starts failed!!"
            exit_program
        fi
    done
    echo "$pod_name"
}

tpcc_prepare() {
    cmd="drop database if exists tpcc_test;"

    if leader_exec_command "$cmd"; then
        leader_pod_idx=$(get_leader_pod_idx)
        export HOST=${NAME_PODS[$leader_pod_idx]}.${SVC_NAME_PODS}
        export DB_USERNAME="root"
        export DB_PASSWORD="1234"
        prepare_cmd="envsubst < serverless/tpcc_prepare.yaml | kubectl create -f -"
        prepare_output=$(eval "$prepare_cmd")
        pod_name=$(echo "$prepare_output" | awk -F'[/ ]' '{print $(NF-1)}')
        time_cnt=1
        while true; do
            prepare_pod_status=$(kubectl get pod | (grep "$pod_name" || true) | awk '{print $3}')
            echo "prepare_pod_status:$prepare_pod_status"
            if [[ "$prepare_pod_status" == "Completed" ]]; then
                break
            elif [[ "$prepare_pod_status" == "Running" || "$prepare_pod_status" == "ContainerCreating" ]]; then
                sleep 1
            else
                if [[ "$time_cnt" -gt 10 ]]; then
                    echo "Tpcc prepare failed!!"
                    exit_program
                else
                    time_cnt=$(($time_cnt + 1))
                    sleep 1
                fi
            fi
        done
        if tpcc_check; then
            echo "Data prepare successfully."
            return 0
        else
            echo "Data prepare fail, try to prepare again."
            return 1
        fi
    else
        echo "cleanup tpcc failed!!"
        exit_program
    fi
    return 0
}

test_kill_leader_container() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    cmd="envsubst < fault_yaml/kill_leader_container.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
}

test_kill_follower_container() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod=${NAME_PODS[$foll1_pod_idx]}
    cmd="envsubst < fault_yaml/kill_follower_container.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    follower_pod=
}

test_kill_2follower_container() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    cmd="envsubst < fault_yaml/kill_2follower_container.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    follower_pod1=
    follower_pod2=
}
test_kill_1leader1follower_container() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    cmd="envsubst < fault_yaml/kill_1leader1follower_container.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
    follower_pod1=
}

test_kill_all_container() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    cmd="envsubst < fault_yaml/kill_all_container.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
    follower_pod1=
    follower_pod2=
}

test_stress_cpu_leader() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    cmd="envsubst < fault_yaml/stress_cpu_leader.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
}

test_stress_memory_leader() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    cmd="envsubst < fault_yaml/stress_memory_leader.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
}

test_stress_cpu_follower() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod=${NAME_PODS[$foll1_pod_idx]}
    cmd="envsubst < fault_yaml/stress_cpu_follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    follower_pod=
}

test_stress_memory_follower() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod=${NAME_PODS[$foll1_pod_idx]}
    cmd="envsubst < fault_yaml/stress_memory_follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    follower_pod=
}

test_stress_cpu_1leader1follower() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod=${NAME_PODS[$foll1_pod_idx]}
    cmd="envsubst < fault_yaml/stress_cpu_1leader1follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
    follower_pod=
}

test_stress_memory_1leader1follower() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod=${NAME_PODS[$foll1_pod_idx]}
    cmd="envsubst < fault_yaml/stress_memory_1leader1follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
    follower_pod=
}
test_stress_cpu_2follower() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    cmd="envsubst < fault_yaml/stress_cpu_2follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    follower_pod1=
    follower_pod2=
}

test_stress_memory_2follower() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    cmd="envsubst < fault_yaml/stress_memory_2follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    follower_pod1=
    follower_pod2=
}

test_stress_cpu_all() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    cmd="envsubst < fault_yaml/stress_cpu_all.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
    follower_pod1=
    follower_pod2=
}

test_stress_memory_all() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    cmd="envsubst < fault_yaml/stress_memory_all.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    leader_pod=
    follower_pod1=
    follower_pod2=
}

test_io_leader() {
    export errorno=$1
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    cmd="envsubst < fault_yaml/io_leader.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    errorno=
    leader_pod=
    datapath=
}
test_io_follower() {
    export errorno=$1
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export datapath=$(get_datapath)
    cmd="envsubst < fault_yaml/io_follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    errorno=
    follower_pod1=
    datapath=
}
test_io_2follower() {
    export errorno=$1
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    export datapath=$(get_datapath)
    cmd="envsubst < fault_yaml/io_2follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    errorno=
    follower_pod1=
    follower_pod2=
    datapath=
}

test_io_1leader1follower() {
    export errorno=$1
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    foll1_pod_idx=$(get_follower1_pod_idx)
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export datapath=$(get_datapath)
    cmd="envsubst < fault_yaml/io_1leader1follower.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    errorno=
    leader_pod=
    follower_pod1=
    datapath=
}

test_io_all() {
    export errorno=$1
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    leader_pod_idx=$(get_leader_pod_idx)
    export leader_pod=${NAME_PODS[$leader_pod_idx]}
    export follower_pod1=${NAME_PODS[$foll1_pod_idx]}
    export follower_pod2=${NAME_PODS[$foll2_pod_idx]}
    export datapath=$(get_datapath)
    cmd="envsubst < fault_yaml/io_all.yaml | kubectl create -f -"
    eval_cmd "$cmd"
    errorno=
    leader_pod=
    follower_pod1=
    follower_pod2=
    datapath=
}

test_network_leader() {
    faulttype=$*
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export pod_name=${NAME_PODS[$leader_pod_idx]}
    if [[ "$faulttype" == "partition" ]]; then
        cmd="envsubst < fault_yaml/network_onepod_partition.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "loss"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_loss.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "delay"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_delay.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "corrupt"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_corrupt.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "duplicate"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_duplicate.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "bandwidth"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_bandwidth.yaml | kubectl create -f -"
    fi
    eval_cmd "$cmd"
    pod_name=
}
test_network_follower() {
    faulttype=$*
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    export pod_name=${NAME_PODS[$foll1_pod_idx]}
    if [[ "$faulttype" == "partition" ]]; then
        cmd="envsubst < fault_yaml/network_onepod_partition.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "loss"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_loss.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "delay"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_delay.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "corrupt"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_corrupt.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "duplicate"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_duplicate.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "bandwidth"* ]]; then
        cmd="envsubst < fault_yaml/network_onepod_bandwidth.yaml | kubectl create -f -"
    fi
    eval_cmd "$cmd"
    pod_name=
}
test_network_2follower() {
    faulttype=$*
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    export pod_name1=${NAME_PODS[$foll1_pod_idx]}
    export pod_name2=${NAME_PODS[$foll2_pod_idx]}
    if [[ "$faulttype" == "partition" ]]; then
        cmd="envsubst < fault_yaml/network_twopod_partition.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "loss"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_loss.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "delay"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_delay.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "corrupt"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_corrupt.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "duplicate"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_duplicate.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "bandwidth"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_bandwidth.yaml | kubectl create -f -"
    fi
    eval_cmd "$cmd"
    pod_name1=
    pod_name2=
}
test_network_1leader1follower() {
    faulttype=$*
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    export pod_name1=${NAME_PODS[$leader_pod_idx]}
    foll1_pod_idx=$(get_follower1_pod_idx)
    export pod_name2=${NAME_PODS[$foll1_pod_idx]}
    if [[ "$faulttype" == "partition" ]]; then
        cmd="envsubst < fault_yaml/network_twopod_partition.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "loss"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_loss.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "delay"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_delay.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "corrupt"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_corrupt.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "duplicate"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_duplicate.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "bandwidth"* ]]; then
        cmd="envsubst < fault_yaml/network_twopod_bandwidth.yaml | kubectl create -f -"
    fi
    eval_cmd "$cmd"
    pod_name1=
    pod_name2=
}

test_network_all() {
    faulttype=$*
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    leader_pod_idx=$(get_leader_pod_idx)
    export pod_name1=${NAME_PODS[$leader_pod_idx]}
    export pod_name2=${NAME_PODS[$foll1_pod_idx]}
    export pod_name3=${NAME_PODS[$foll2_pod_idx]}
    if [[ "$faulttype" == "partition" ]]; then
        cmd="envsubst < fault_yaml/network_threepod_partition.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "loss"* ]]; then
        cmd="envsubst < fault_yaml/network_threepod_loss.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "delay"* ]]; then
        cmd="envsubst < fault_yaml/network_threepod_delay.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "corrupt"* ]]; then
        cmd="envsubst < fault_yaml/network_threepod_corrupt.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "duplicate"* ]]; then
        cmd="envsubst < fault_yaml/network_threepod_duplicate.yaml | kubectl create -f -"
    elif [[ "$faulttype" == "bandwidth"* ]]; then
        cmd="envsubst < fault_yaml/network_threepod_bandwidth.yaml | kubectl create -f -"
    fi
    eval_cmd "$cmd"
    pod_name1=
    pod_name2=
    pod_name3=
}

test_data_lost_leader() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    fault_cmd="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$leader_pod_idx]} bash"
    eval "$fault_cmd"
    delete_cmd="kubectl delete pod ${NAME_PODS[$leader_pod_idx]}"
    eval "$delete_cmd"
}

test_data_lost_follower() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    fault_cmd="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$foll1_pod_idx]} bash"
    eval "$fault_cmd"
    delete_cmd="kubectl delete pod ${NAME_PODS[$foll1_pod_idx]}"
    eval "$delete_cmd"
}

test_data_lost_follower2() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower2_pod_idx)
    fault_cmd="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$foll1_pod_idx]} bash"
    eval "$fault_cmd"
    delete_cmd="kubectl delete pod ${NAME_PODS[$foll1_pod_idx]}"
    eval "$delete_cmd"
}

test_data_lost_2follower() {
    echo "Start $test_fault_name"
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    fault_cmd1="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$foll1_pod_idx]} bash"
    fault_cmd2="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$foll2_pod_idx]} bash"
    eval "$fault_cmd1"
    eval "$fault_cmd2"
}

test_data_lost_1leader_1follower() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    foll1_pod_idx=$(get_follower1_pod_idx)
    fault_cmd1="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$leader_pod_idx]} bash"
    fault_cmd2="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$foll1_pod_idx]} bash"
    eval "$fault_cmd1"
    eval "$fault_cmd2"
}

test_data_lost_all() {
    echo "Start $test_fault_name"
    leader_pod_idx=$(get_leader_pod_idx)
    foll1_pod_idx=$(get_follower1_pod_idx)
    foll2_pod_idx=$(get_follower2_pod_idx)
    fault_cmd1="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$leader_pod_idx]} bash"
    fault_cmd2="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$foll1_pod_idx]} bash"
    fault_cmd3="echo 'rm -rf /data/mysql/data' | kubectl exec -it ${NAME_PODS[$foll2_pod_idx]} bash"
    eval "$fault_cmd1"
    eval "$fault_cmd2"
    eval "$fault_cmd3"
}

create_table() {
    cmd="SET GLOBAL max_connect_errors=10000;set global max_connections = 2000;set @@global.net_read_timeout=3600;"
    pod_exec_command "$cmd" 0
    pod_exec_command "$cmd" 1
    pod_exec_command "$cmd" 2
    while true; do
        if tpcc_prepare; then
            break
        fi
    done
    cmd="CREATE DATABASE IF NOT EXISTS mydb; use mydb; DROP TABLE IF EXISTS tmp_table; CREATE TABLE IF NOT EXISTS tmp_table (id INT PRIMARY KEY AUTO_INCREMENT, value VARCHAR(255)); INSERT INTO tmp_table (value) VALUES (0);"
    if ! leader_exec_command "$cmd"; then
        echo "Create table failed"
        return 1
    fi
    return 0
}

check_cluster() {
    component="mysql"

    cmd="SELECT COUNT(DISTINCT match_index)=1 AND COUNT(match_index)=3 FROM information_schema.wesql_cluster_global where role = 'Leader' or role = 'Follower'"
    if ! res=$(leader_exec_command "$cmd"); then
        return 1
    fi
    if [ "$res" == "1" ]; then
        return 0
    else
        echo "Cluster abnormal"
        return 1
    fi
}

connect_cluster() {
    component="mysql"

    cmd="use mydb; SELECT value FROM tmp_table WHERE id = 1;"
    if ! res=$(leader_exec_command "$cmd"); then
        return 1
    fi
    echo "$res---$DEFAULT_VALUE"
    if [ "$res" == "$DEFAULT_VALUE" ]; then
        cmd="use mydb; UPDATE tmp_table SET value = value + 1 WHERE id = 1;"
        if leader_exec_command "$cmd"; then
            cmd="use mydb; SELECT value FROM tmp_table WHERE id = 1;"
            DEFAULT_VALUE=$(leader_exec_command "$cmd")
            return 0
        else
            return 1
        fi
    elif [ "$res" == "$((DEFAULT_VALUE + 1))" ]; then
        ((DEFAULT_VALUE = DEFAULT_VALUE + 1))
        return 0
    else
        echo "$res---$DEFAULT_VALUE"
        RPO="Data lost"
        echo "Data lost"
        exit_program
    fi
}

add_test_result() {
    test_result=${1:-""}
    if [[ $RET_FLAG -eq 2 ]]; then
        test_result="【PASSED】|【"$test_result"】|【RTO:"$RTO"#RPO:"$RPO"】|【FullRecovery:"$fullrecov"】"
    elif [[ $RET_FLAG -eq 3 ]]; then
        test_result="【SKIPPED】|【"$test_result"】|【RTO:"$RTO"#RPO:"$RPO"】|【FullRecovery:"$fullrecov"】"
    else
        test_result="【FAILED】|【"$test_result"】|【RTO:"$RTO"#RPO:"$RPO"】|【FullRecovery:"$fullrecov"】"
    fi
    if [[ "$TEST_RESULT" != *"$test_result"* ]]; then
        TEST_RESULT="$TEST_RESULT##$test_result"
        RET_FLAG=1
    fi
}

report_test_result() {
    echo "report test result"
    for test_ret in $(echo "$TEST_RESULT" | sed 's/##/ /g'); do
        test_ret=$(echo "$test_ret" | sed 's/#/ /g')
        case $test_ret in
        *【PASSED】*)
            echo "$(tput -T xterm setaf 2)$test_ret$(tput -T xterm sgr0)"
            ;;
        *【SKIPPED】*)
            echo "$(tput -T xterm setaf 3)$test_ret$(tput -T xterm sgr0)"
            ;;
        *【FAILED】*)
            echo "$(tput -T xterm setaf 1)$test_ret$(tput -T xterm sgr0)"
            EXIT_FLAG=1
            ;;
        *)
            echo "$test_ret"
            ;;
        esac
    done
}

check_rto_rpo() {
    local lossfault="loss --loss 100"
    local delayfault="delay --latency=15s -c=100 --jitter=0ms"
    local corruptfault="corrupt --corrupt=100 -c=100"
    local duplicatefault="duplicate --duplicate=100 -c=100"
    local bandwidthfault="bandwidth --rate=1kbps"
    local uname=$(uname -s)
    echo "Start program to monitor the cluster service status."

    for i in $((RANDOM % 2)); do
        connect_cluster
    done

   # for fault_rand in {1..5} {56..58}; do
    for fault_rand in {1..61}; do
        RTO=0
        count=0
        everstopped="false"
        podeverstopped="false"
        fullrecov=0
        running_pod_name=$(tpcc_run)
        continue_flag=0

        case $fault_rand in
        1)
            test_fault_name="kill_leader_container"
            test_kill_leader_container
            ;;
        2)
            test_fault_name="kill_follower_container"
            test_kill_follower_container
            ;;
        3)
            test_fault_name="kill_2follower_container"
            test_kill_2follower_container
            ;;
        4)
            test_fault_name="kill_1leader1follower_container"
            test_kill_1leader1follower_container
            ;;
        5)
            test_fault_name="kill_all_container"
            test_kill_all_container
            ;;
        6)
            test_fault_name="stress_cpu_leader"
            test_stress_cpu_leader
            ;;
        7)
            test_fault_name="stress_mem_leader"
            test_stress_memory_leader
            ;;
        8)
            test_fault_name="stress_cpu_follower"
            test_stress_cpu_follower
            ;;
        9)
            test_fault_name="stress_memory_follower"
            test_stress_memory_follower
            ;;
        10)
            test_fault_name="stress_cpu_1leader1follower"
            test_stress_cpu_1leader1follower
            ;;
        11)
            test_fault_name="stress_memory_1leader1follower"
            test_stress_memory_1leader1follower
            ;;
        12)
            test_fault_name="stress_cpu_2follower"
            test_stress_cpu_2follower
            ;;
        13)
            test_fault_name="stress_memory_2follower"
            test_stress_memory_2follower
            ;;
        14)
            test_fault_name="stress_cpu_all"
            test_stress_cpu_all
            ;;
        15)
            test_fault_name="stress_memory_all"
            test_stress_memory_all
            ;;
        16)
            test_fault_name="test_io_leader_28Nospaceleft"
            test_io_leader 28
            ;;
        17)
            test_fault_name="test_io_follower_28Nospaceleft"
            test_io_follower 28
            ;;
        18)
            test_fault_name="test_io_2follower_28Nospaceleft"
            test_io_2follower 28
            ;;
        19)
            test_fault_name="test_io_1leader1follower_28Nospaceleft"
            test_io_1leader1follower 28
            ;;
        20)
            test_fault_name="test_io_all_28Nospaceleft"
            test_io_all 28
            ;;
        21)
            test_fault_name="test_io_leader_6Nosuchdevice"
            test_io_leader 6
            ;;
        22)
            test_fault_name="test_io_follower_6Nosuchdevice"
            test_io_follower 6
            ;;
        23)
            test_fault_name="test_io_2follower_6Nosuchdevice"
            test_io_2follower 6
            ;;
        24)
            test_fault_name="test_io_1leader1follower_6Nosuchdevice"
            test_io_1leader1follower 6
            ;;
        25)
            test_fault_name="test_io_all_6Nosuchdevice"
            test_io_all 6
            ;;
        26)
            test_fault_name="test_network_leader_partition"
            test_network_leader partition
            ;;
        27)
            test_fault_name="test_network_follower_partition"
            # test_network_follower partition
            continue_flag=1
            ;;
        28)
            test_fault_name="test_network_2follower_partition"
            # test_network_2follower partition
            continue_flag=1
            ;;
        29)
            test_fault_name="test_network_1leader1follower_partition"
            # test_network_1leader1follower partition
            continue_flag=1
            ;;
        30)
            test_fault_name="test_network_all_partition"
            # test_network_all partition
            continue_flag=1
            ;;
        31)
            test_fault_name="test_network_leader_loss100"
            test_network_leader $lossfault
            ;;
        32)
            test_fault_name="test_network_follower_loss100"
            test_network_follower $lossfault
            ;;
        33)
            test_fault_name="test_network_2follower_loss100"
            test_network_2follower $lossfault
            ;;
        34)
            test_fault_name="test_network_1leader1follower_loss100"
            test_network_1leader1follower $lossfault
            ;;
        35)
            test_fault_name="test_network_all_loss100"
            test_network_all $lossfault
            ;;
        36)
            test_fault_name="test_network_leader_delay100"
            test_network_leader $delayfault
            ;;
        37)
            test_fault_name="test_network_follower_delay100"
            test_network_follower $delayfault
            ;;
        38)
            test_fault_name="test_network_2follower_delay100"
            test_network_2follower $delayfault
            ;;
        39)
            test_fault_name="test_network_1leader1follower_delay100"
            test_network_1leader1follower $delayfault
            ;;
        40)
            test_fault_name="test_network_all_delay100"
            test_network_all $delayfault
            ;;
        41)
            test_fault_name="test_network_leader_corrupt100"
            test_network_leader $corruptfault
            ;;
        42)
            test_fault_name="test_network_follower_corrupt100"
            test_network_follower $corruptfault
            ;;
        43)
            test_fault_name="test_network_2follower_corrupt100"
            test_network_2follower $corruptfault
            ;;
        44)
            test_fault_name="test_network_1leader1follower_corrupt100"
            test_network_1leader1follower $corruptfault
            ;;
        45)
            test_fault_name="test_network_all_corrupt100"
            test_network_all $corruptfault
            ;;
        46)
            test_fault_name="test_network_leader_duplicate100"
            test_network_leader $duplicatefault
            ;;
        47)
            test_fault_name="test_network_follower_duplicate100"
            test_network_follower $duplicatefault
            ;;
        48)
            test_fault_name="test_network_2follower_duplicate100"
            test_network_2follower $duplicatefault
            ;;
        49)
            test_fault_name="test_network_1leader1follower_duplicate100"
            test_network_1leader1follower $duplicatefault
            ;;
        50)
            test_fault_name="test_network_all_duplicate100"
            test_network_all $duplicatefault
            ;;
        51)
            test_fault_name="test_network_leader_bandwith100"
            test_network_leader $bandwidthfault
            ;;
        52)
            test_fault_name="test_network_follower_bandwith100"
            test_network_follower $bandwidthfault
            ;;
        53)
            test_fault_name="test_network_2follower_bandwith100"
            test_network_2follower $bandwidthfault
            ;;
        54)
            test_fault_name="test_network_1leader1follower_bandwith100"
            test_network_1leader1follower $bandwidthfault
            ;;
        55)
            test_fault_name="test_network_all_bandwith100"
            test_network_all $bandwidthfault
            ;;
        56)
            test_fault_name="test_data_lost_leader"
            test_data_lost_leader
            ;;
        57)
            test_fault_name="test_data_lost_follower"
            test_data_lost_follower
            ;;
        58)
            test_fault_name="test_data_lost_follower2"
            test_data_lost_follower2
            ;;
        59)
            test_fault_name="test_data_lost_2follower"
            # test_data_lost_2follower
            continue_flag=1
            ;;
        60)
            test_fault_name="test_data_lost_1leader_1follower"
            test_data_lost_1leader_1follower
            ;;
        61)
            test_fault_name="test_data_lost_all"
            test_data_lost_all
            ;;

        esac

        if [ "$continue_flag" -eq 1 ]; then
            continue
        fi

        injectFaultTime=$(date +'%Y-%m-%d %H:%M:%S')
        while true; do
            check_pod_running
            if connect_cluster; then
                count=$(($count + 1))
                echo "Connect cluster $(date +'%Y-%m-%d %H:%M:%S')"
                if [ "$runningToStopTime" != "null" ]; then
                    stopToRunningTime=$(date +'%Y-%m-%d %H:%M:%S')
                    echo "runningToStopTime - $runningToStopTime"
                    echo "stopToRunningTime - $stopToRunningTime"
                    if [[ "$uname" == "Darwin" ]]; then
                        interval=$(($(date -j -f "%Y-%m-%d %H:%M:%S" "$stopToRunningTime" +%s) - $(date -j -f "%Y-%m-%d %H:%M:%S" "$runningToStopTime" +%s)))
                    else
                        interval=$(($(date -d "$stopToRunningTime" +%s) - $(date -d "$runningToStopTime" +%s)))
                    fi
                    echo "Time interval since $cluster_name started: $interval seconds"
                    RTO=$(($interval))

                    check_cluster_status
                    if [[ $interval -gt 30 ]]; then
                        RET_FLAG=1
                    else
                        RET_FLAG=2
                    fi

                    if tpcc_check; then
                        echo "tpcc check pass"
                        RPO=0
                    else
                        exit_program
                    fi
                    podrunningToStopTime="null"
                    podStopToRunningTime="null"
                    runningToStopTime="null"
                    break
                fi
            else
                echo "Fail to connect cluster $(date +'%Y-%m-%d %H:%M:%S')"
                everstopped="true"
                if [ "$runningToStopTime" == "null" ]; then
                    runningToStopTime=$(date +'%Y-%m-%d %H:%M:%S')
                    echo "runningToStopTime - $runningToStopTime"
                fi
            fi
            if [[ "$everstopped" == "false" && "$pod0_status" == "Running" && "$pod1_status" == "Running" && "$pod2_status" == "Running" && "$count" -gt 3 ]]; then
                if [[ "$podeverstopped" == "true" ]]; then
                    echo "podStopToRunningTime:$podStopToRunningTime"
                    echo "podrunningToStopTime:$podrunningToStopTime"
                    if [[ "$uname" == "Darwin" ]]; then
                        fullrecov=$(($(date -j -f "%Y-%m-%d %H:%M:%S" "$podStopToRunningTime" +%s) - $(date -j -f "%Y-%m-%d %H:%M:%S" "$podrunningToStopTime" +%s)))
                    else
                        fullrecov=$(($(date -d "$podStopToRunningTime" +%s) - $(date -d "$podrunningToStopTime" +%s)))
                    fi

                    echo "fullRecoveryTime is:"$fullrecov
                    podrunningToStopTime="null"
                    podStopToRunningTime="null"
                fi
                RTO=0
                RET_FLAG=2
                if tpcc_check; then
                    echo "tpcc check pass"
                    RPO=0
                else
                    RPO="Data inconsistent"
                    echo "Data inconsistent!!"
                    exit_program
                fi
                break
            fi
            sleep 5
        done
        running_pod_state=$(kubectl get pod | (grep "$running_pod_name" || true) | awk '{print $3}')
        echo "Deleting tpcc run pod: ${running_pod_name}, which is in ${running_pod_state} state."
        delete_cmd="kubectl delete pod ${running_pod_name}"
        eval "$delete_cmd"
        if [ $? -ne 0 ]; then
            echo "Executing command \"$delete_cmd\" failed!!"
        fi
        add_test_result "$test_fault_name"
        echo "fullRecoveryTime is:"$fullrecov
        while true; do
            if check_cluster; then
                break
            fi
        done
        report_test_result
    done

}

delete_resource() {
    cmd="kubectl delete svc ${CLUSTER_NAME}-headless"
    eval_cmd "$cmd"
    cmd="kubectl delete sts ${CLUSTER_NAME}-0"
    eval_cmd "$cmd"
    cmd="kubectl delete sts ${CLUSTER_NAME}-1"
    eval_cmd "$cmd"
    cmd="kubectl delete sts ${CLUSTER_NAME}-2"
    eval_cmd "$cmd"
    cmd="kubectl delete pvc data-${CLUSTER_NAME}-0-0"
    eval_cmd "$cmd"
    cmd="kubectl delete pvc data-${CLUSTER_NAME}-1-0"
    eval_cmd "$cmd"
    cmd="kubectl delete pvc data-${CLUSTER_NAME}-2-0"
    eval_cmd "$cmd"
}

generate_global_names() {
    suffix=""
    if [[ -z "$CLUSTER_NAME" ]]; then
        for i in {1..6}; do
            index=$(( $RANDOM % 26 ))
            lower=${LOWERS:$index:1}
            if [[ -z "$suffix" ]]; then
                suffix=$lower
            else
                suffix=$suffix$lower
            fi
        done
        CLUSTER_NAME=$DEFAULT_CLUSTER_NAME-$suffix
        export CLUSTER_NAME
    fi
    echo "[CLUSTER_NAME]$CLUSTER_NAME[CLUSTER_NAME]"

    NAME_PODS=("${CLUSTER_NAME}-0-0" "${CLUSTER_NAME}-1-0" "${CLUSTER_NAME}-2-0")
    SVC_NAME_PODS="${CLUSTER_NAME}-headless"
}

main() {
    export duration="2m"
    export NAMESPACE=${DEFAULT_NAMESPACE}
    export CLUSTER_NAME=${DEFAULT_CLUSTER_NAME}
    export OBJSTORE_PROVIDER=${DEFAULT_PROVIDER}
    export OBJSTORE_REGION=${DEFAULT_REGION}
    export OBJSTORE_BUCKET=${DEFAULT_BUCKET}
    export OBJSTORE_ENDPOINT=${DEFAULT_ENDPOINT}
    export OBJSTORE_USE_HTTPS=${DEFAULT_USE_HTTPS}
    export WESQL_IMAGE=${DEFAULT_IMAGE}
    export ACCESS_KEY=${DEFAULT_AK}
    export SECRET_KEY=${DEFAULT_SK}
    export LOWERS="abcdefghijklmnopqrstuvwxyz"

    parse_command_line "$@"
    echo "bash test_fault_serverless.sh --threads ${THREADS} \
        --tables ${TABLES} --size/scale ${SIZE} --duration ${duration} \
        --namespace ${NAMESPACE}"

    generate_global_names
    create_cluster

    cmd='kubectl delete validatingwebhookconfigurations.admissionregistration.k8s.io chaos-mesh-validation-auth'
    eval_cmd "$cmd"
    cmd="kubectl delete IOChaos \$(kubectl get IOChaos |awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete PodChaos \$(kubectl get PodChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete NetworkChaos \$(kubectl get NetworkChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete StressChaos \$(kubectl get StressChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete AWSChaos \$(kubectl get AWSChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete GCPChaos \$(kubectl get GCPChaos | awk '{print \$1}')"
    eval_cmd "$cmd"
    cmd="kubectl delete pod \$(kubectl get pod | grep "sysbench-tpcc-" | awk '{print \$1}')"
    eval_cmd "$cmd"

    if create_table; then
        echo "The database is successfully inited."
    else
        echo "Failed to create table in db, please check the reason."
    fi

    check_rto_rpo
    delete_resource
    report_test_result
}

parse_command_line() {
    while :; do
        case "${1:-}" in
        -h | --help)
            show_help
            exit
            ;;
        -n | --namespace)
            NAMESPACE="$2"
            shift
            ;;
        -cn|--cluster-name)
            CLUSTER_NAME="$2"
            shift
            ;;
        -op|--objstore-provider)
            OBJSTORE_PROVIDER="$2"
            shift
            ;;
        -or|--objstore-region)
            OBJSTORE_REGION="$2"
            shift
            ;;
        -ob|--objstore-bucket)
            OBJSTORE_BUCKET="$2"
            shift
            ;;
        -oe|--objstore-endpoint)
            OBJSTORE_ENDPOINT="$2"
            shift
            ;;
        -ouh|--objstore-use-https)
            OBJSTORE_USE_HTTPS="$2"
            shift
            ;;
        -wi|--wesql-image)
            WESQL_IMAGE="$2"
            shift
            ;;
        -ak64|--access-key-base64)
            ACCESS_KEY=$(echo -n "$2" | base64)
            shift
            ;;
        -sk64|--secret-key-base64)
            SECRET_KEY=$(echo -n "$2" | base64)
            shift
            ;;
        -th | --threads)
            THREADS="$2"
            shift
            ;;
        -tb | --tables)
            TABLES="$2"
            shift
            ;;
        -sz | -sc | --size | --scale)
            SIZE="$2"
            shift
            ;;
        -d | --duration)
            duration="$2"
            shift
            ;;
        *)
            break
            ;;
        esac
        shift
    done
}

main "$@"
