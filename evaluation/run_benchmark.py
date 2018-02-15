import subprocess
import sys
import os
import time

WORKER_IP = '' # IP of worker
# The following are all on the OL worker machine
WORKER_USER= '' # User to ssh as to perform OL worker startup, teardown
RM_LOC = '' # Path to teardown script
RUN_LOC = '' # Path to startup script
CLUSTER_LOC = '' # Path to OL worker directory
OL_LOC = '' # Path to  open-lambda src code repo
TMP_CONF = '' # Path to where worker config will be copied to for startup

ssh_conf = '%s@%s' % (WORKER_USER, WORKER_IP)

def teardown():
    print 'killing and remvoing old cluster'
    cmd = "bash -c '%s ; rm -rf %s'" % (RM_LOC, CLUSTER_LOC)
    try:
        subprocess.check_output(["ssh", ssh_conf, cmd])
    except:
        print 'error removing old cluster, may not have existed, continuing'


def remove_tmp_conf():
    print 'removing old cluster conf'
    cmd = "bash -c 'rm %s'" % TMP_CONF
    try:
        subprocess.check_output(["ssh", ssh_conf, cmd])
    except:
        print 'error removing old cluster conf, continuing'


def copy_temp_conf(cluster_conf):
    print 'copying new cluster conf %s' % cluster_conf
    cmd = '%s:%s' % (ssh_conf, TMP_CONF)
    subprocess.check_output(["scp", "./benchmark_examples/cluster_confs/%s" % cluster_conf, cmd])

def start_cluster():
    print 'starting new cluster'
    cmd = "bash -c '%s %s'" % (RUN_LOC, TMP_CONF)
    subprocess.check_output(["ssh", ssh_conf, cmd])

def run_sub_benchmark(benchmark_logs_dir, cluster_conf, workload_conf, cluster_name, workload_name):
    print 'running benchmark %s' % workload_name
    log_name = 'bench.log'
    print workload_conf
    cmd = ' '.join(["python3", "run_workload.py",  "./workload_examples/runner_confs/%s" % workload_conf, '%s/%s' % (benchmark_logs_dir, log_name)])
    print cmd
    time.sleep(10)
    os.system(cmd)

def capture_confs(benchmark_logs_dir, cluster_conf, workload_conf):
    print 'capturing confs used'
    subprocess.check_output(["cp", "./benchmark_examples/cluster_confs/%s" % cluster_conf, benchmark_logs_dir])
    subprocess.check_output(["cp", "./workload_examples/runner_confs/%s" % workload_conf, benchmark_logs_dir])

def capture_worker_logs(benchmark_logs_dir):
    print 'gathering worker logs'
    worker_logs_loc = '%s/logs/worker-0.out' % CLUSTER_LOC
    remote_path = '%s:%s' % (ssh_conf, worker_logs_loc)
    subprocess.check_output(["scp", remote_path, benchmark_logs_dir])

def run_tests(benchmark_logs_dir, clusters, workloads):
    for cluster_conf in clusters:
        for workload_conf in workloads:
            cluster_name = cluster_conf.split('.')[0]
            workload_name = workload_conf.split('.')[0]

            sub_benchmark_logs_dir = '%s/%s-%s' % (benchmark_logs_dir, cluster_name, workload_name)
            os.makedirs(sub_benchmark_logs_dir)

            teardown()
            teardown()
            remove_tmp_conf()
            copy_temp_conf(cluster_conf)
            start_cluster()
            try:
                run_sub_benchmark(sub_benchmark_logs_dir, cluster_conf, workload_conf, cluster_name, workload_name)
            except:
                pass
            capture_confs(sub_benchmark_logs_dir, cluster_conf, workload_conf)
            capture_worker_logs(sub_benchmark_logs_dir)

def parse_csv(benchmark_conf):
    benchmark_conf_file = open(benchmark_conf)
    clusters = benchmark_conf_file.readline().strip().split(',')
    workloads = benchmark_conf_file.readline().strip().split(',')
    print 'clusters: ' + str(clusters)
    print 'workloads: ' + str(workloads)
    return (clusters, workloads)

def main():
    if len(sys.argv) != 1:
        print 'usage: <benchmark_spec.>'

    benchmark_conf = sys.argv[1]
    benchmark_name = benchmark_conf.split('.')[0]

    r = parse_csv(benchmark_conf)
    clusters = r[0]
    workloads = r[1]

    benchmark_logs_dir = 'logs/%s/%d' % (benchmark_name, time.time())
    os.makedirs(benchmark_logs_dir)

    run_tests(benchmark_logs_dir, clusters, workloads)

    print 'done'

main()
