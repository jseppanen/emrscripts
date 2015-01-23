#!/usr/bin/env python

import boto
import boto
import boto.emr
from boto.s3.key import Key
from boto.emr.bootstrap_action import BootstrapAction
from boto.emr.instance_group import InstanceGroup
from boto.emr.step import ScriptRunnerStep
import time
import os
import sys
import argparse
import subprocess

# emr run <pig-script> [path]
# - run with monitoring, sync results
# emr add <pig-script>
# - add step without monitoring
# emr sync <pig-script> [path]
# - get results of script
# emr ssh
# - ssh to master
# emr terminate
# - terminate clusters
# emr kill <pig-script>
# - kill step

def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    parser_add = subparsers.add_parser('add',
        description='Add a step')
    parser_add.add_argument('script')
    parser_add.add_argument('-p', dest='parallel', action='store_true',
        help='launch in parallel with currently running steps')
    parser_add.set_defaults(func=cmd_add)
    parser_run = subparsers.add_parser('run',
        description='Run script')
    parser_run.add_argument('script')
    parser_run.add_argument('path', nargs='?')
    parser_run.add_argument('-p', dest='parallel', action='store_true',
        help='launch in parallel with currently running steps')
    parser_run.set_defaults(func=cmd_run)
    parser_ssh = subparsers.add_parser('ssh',
        description='SSH to master')
    parser_ssh.set_defaults(func=cmd_ssh)
    return parser.parse_args()

# upload script to s3
# clean existing results from s3, if any
# use jarno-interactive cluster, if exists
# launch script
# monitor progress
# set up ssh tunnel to job tracker
# sync results back, concatenate to single file

def main():
    global s3_conn, emr_conn
    confpath = os.path.join(os.path.dirname(__file__), 'emr.conf.py')
    conf = exec(open(confpath).read(), globals())
    args = parse_args()
    s3_conn = boto.connect_s3()
    emr_conn = boto.emr.connect_to_region('us-east-1')
    args.func(args)
    s3_conn.close()
    emr_conn.close()

def cmd_add(args):
    script_uri = upload_script(args.script)
    try:
        jobid = find_cluster(vacant=args.parallel)
    except NotFoundError:
        jobid = launch_cluster(args.script)
    add_step(jobid, args.script, script_uri)

def cmd_run(args):
    script_uri = upload_script(args.script)
    try:
        jobid = find_cluster(vacant=args.parallel)
    except NotFoundError:
        jobid = launch_cluster(args.script)
    add_step(jobid, args.script, script_uri)
    wait(jobid)

def cmd_ssh(args):
    jobid = find_cluster()
    host = emr_conn.describe_jobflow(jobid).masterpublicdnsname
    ssh(host)

def upload_script(path):
    '''upload script to s3'''
    k = Key(s3_conn.get_bucket(bucket_name))
    k.key = 'emrunner/' + path
    k.set_contents_from_file(open(path))
    script_uri = 's3://%s/emrunner/%s' % (bucket_name, path)
    return script_uri

def find_cluster(vacant=False):
    '''find previous cluster or launch new if not found'''
    states = ['STARTING', 'BOOTSTRAPPING', 'WAITING']
    if not vacant:
        # launch sequentially
        states += ['RUNNING']
    jobids = [c.id for c in emr_conn.list_clusters(
                  cluster_states=states).clusters
              if c.name == default_cluster_name]
    if jobids:
        return jobids[0]
    raise NotFoundError(default_cluster_name)

def launch_cluster(script_name):
    '''launch new cluster'''
    instance_groups = [
        InstanceGroup(1, 'MASTER', 'm2.4xlarge', 'ON_DEMAND', 'MASTER_GROUP'),
        InstanceGroup(3, 'CORE', 'm2.4xlarge', 'ON_DEMAND', 'CORE_GROUP'),
    ]
    bootstrap_actions = [
        BootstrapAction('install-pig', install_pig_script, [pig_version]),
    ]
    jobid = emr_conn.run_jobflow(
        name=os.environ['USER'] + '-' + script_name,
        keep_alive=False,
        ami_version=ami_version,
        visible_to_all_users=True,
        ec2_keyname=ec2_keyname,
        log_uri=log_uri,
        action_on_failure='CONTINUE',
        instance_groups=instance_groups,
        bootstrap_actions=bootstrap_actions)
    return jobid

def add_step(jobid, script_name, script_uri):
    steps = [
        ScriptRunnerStep(script_name, step_args=
            ['/home/hadoop/pig/bin/pig', '-f', script_uri, '-l', '.'])
    ]
    emr_conn.add_jobflow_steps(jobid, steps)

def wait(jobid):
    status = 'asdf'
    while status != 'TERMINATED' and status != 'WAITING':
        status = emr_conn.describe_jobflow(jobid).state
        sys.stdout.write('\r%s          ' % status)
        sys.stdout.flush()
        time.sleep(5)

    # ssh -i ~/.ssh/my.pem -D 8157 hadoop@ec2-1-2-3-4.compute-1.amazonaws.com
    # -o "StrictHostKeyChecking no"
    # tail -f /mnt/var/log/hadoop/steps/s-1111111111111/stderr

    sys.stdout.write('\n')

def ssh(host):
    os.execl('/usr/bin/ssh', 'ssh',
             '-i', pem_path,
             '-o', 'StrictHostKeyChecking=no',
             'hadoop@'+host)

class NotFoundError(BaseException):
    pass

if __name__ == '__main__':
    main()
