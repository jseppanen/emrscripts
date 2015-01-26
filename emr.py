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
# emr proxy
# - Run SOCKS proxy connected to master
# emr sync <pig-script> [path]
# - get results of script
# emr ssh
# - ssh to master
# emr tail
# - tail file from running step on master (default stderr)
# emr terminate
# - terminate clusters
# emr kill <pig-script>
# - kill step

def parse_args():
    parser = argparse.ArgumentParser(
        usage='''emr <command> [ərgs]

The available commands are:
   add        Add a step
   proxy      Launch SOCKS proxy connected to master
   run        Run step
   ssh        SSH to master (launch interactive if not running)
   tail       Tail file from running step on master (default stderr)
   terminate  Terminate the running cluster''')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True
    parser_add = subparsers.add_parser('add',
        description='Add a step')
    parser_add.add_argument('script')
    parser_add.add_argument('-p', dest='parallel', action='store_true',
        help='launch in parallel with currently running steps')
    parser_proxy = subparsers.add_parser('proxy',
        description='Launch SOCKS proxy connected to master')
    parser_run = subparsers.add_parser('run',
        description='Run script')
    parser_run.add_argument('script')
    parser_run.add_argument('path', nargs='?')
    parser_run.add_argument('-p', dest='parallel', action='store_true',
        help='launch in parallel with currently running steps')
    parser_ssh = subparsers.add_parser('ssh',
        description='SSH to master')
    parser_tail = subparsers.add_parser('tail',
        description='Tail file from running step on master (default stderr)')
    parser_tail.add_argument('filename', nargs='?', default='stderr')
    parser_terminate = subparsers.add_parser('terminate',
        description='Terminate clusters')
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
    fun = globals()['cmd_' + args.command]
    fun(args)
    s3_conn.close()
    emr_conn.close()

def cmd_add(args):
    script_uri = upload_script(args.script)
    try:
        jobid = find_cluster(vacant=args.parallel)
    except NotFoundError:
        jobid = launch_cluster(args.script)
    add_step(jobid, args.script, script_uri)

def cmd_proxy(args):
    jobid = find_cluster()
    host = emr_conn.describe_jobflow(jobid).masterpublicdnsname
    ssh(host, opts=['-ND', '8157'])

def cmd_run(args):
    script_uri = upload_script(args.script)
    try:
        jobid = find_cluster(vacant=args.parallel)
    except NotFoundError:
        jobid = launch_cluster(args.script)
    add_step(jobid, args.script, script_uri)
    wait(jobid)

def cmd_ssh(args):
    try:
        jobid = find_cluster()
    except NotFoundError:
        jobid = launch_cluster('interactive', keep_alive=True)
        wait(jobid)
    host = emr_conn.describe_jobflow(jobid).masterpublicdnsname
    ssh(host)

def cmd_tail(args):
    jobid = find_cluster()
    step_id = find_step(jobid)
    host = emr_conn.describe_jobflow(jobid).masterpublicdnsname
    ssh(host, 'tail', '-f', '/mnt/var/log/hadoop/steps/%s/%s' % (step_id, args.filename))

def cmd_terminate(args):
    jobid = find_cluster()
    emr_conn.terminate_jobflow(jobid)

def upload_script(path):
    '''upload script to s3'''
    k = Key(s3_conn.get_bucket(bucket_name))
    k.key = 'emrunner/' + path
    k.set_contents_from_file(open(path))
    script_uri = 's3://%s/emrunner/%s' % (bucket_name, path)
    return script_uri

def find_cluster(vacant=False):
    '''find previous cluster'''
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

def find_step(jobid):
    '''find running step'''
    steps = [s.id for s in emr_conn.list_steps(jobid).steps
             if s.status.state == 'RUNNING']
    if not steps:
        raise NotFoundError('RUNNING')
    assert len(steps) == 1
    return steps[0]

def launch_cluster(script_name, keep_alive=False):
    '''launch new cluster'''
    instance_groups = [
        InstanceGroup(1, 'MASTER', 'm2.4xlarge', 'ON_DEMAND', 'MASTER_GROUP'),
        InstanceGroup(3, 'CORE', 'm2.4xlarge', 'ON_DEMAND', 'CORE_GROUP'),
    ]
    bootstrap_actions = [
        BootstrapAction('install-pig', install_pig_script, [pig_version]),
    ]
    name=os.environ['USER'] + '-' + script_name
    jobid = emr_conn.run_jobflow(
        name=name,
        keep_alive=keep_alive,
        ami_version=ami_version,
        visible_to_all_users=True,
        ec2_keyname=ec2_keyname,
        log_uri=log_uri,
        action_on_failure='CONTINUE',
        instance_groups=instance_groups,
        bootstrap_actions=bootstrap_actions)
    print('launched %s (%s)' % (name, jobid))
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
    sys.stdout.write('\n')

def ssh(host, *args, **kwargs):
    opts = kwargs.pop('opts', [])
    args = ['ssh',
             '-i', pem_path,
             '-o', 'StrictHostKeyChecking=no'] + \
        opts + ['hadoop@'+host] + list(args)
    os.execv('/usr/bin/ssh', args)

class NotFoundError(BaseException):
    pass

if __name__ == '__main__':
    main()
