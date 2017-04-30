#!/usr/bin/env python
import sys

from airflow.configuration import conf
from datetime import datetime
from airflow.cwl_runner.cwldag import CWLDAG
from airflow.cwl_runner.jobdispatcher import JobDispatcher
from airflow.cwl_runner.jobcleanup import JobCleanup
import glob
import os
import shutil
import ruamel.yaml as yaml
import logging
import tempfile
from airflow.cwl_runner.cwlutils import conf_get_default, get_only_files

def get_max_jobs_to_run():
    try:
        max_jobs_to_run = int(conf_get_default('biowardrobe', 'MAX_JOBS_TO_RUN', 1))
    except ValueError:
        logging.error('Error evaluating MAX_JOBS_TO_RUN as integer: {0}'.format (conf_get_default('biowardrobe', 'MAX_JOBS_TO_RUN', 1)))
        sys.exit()
    return max_jobs_to_run

def eval_log_level(key):
    log_depth = {
        'CRITICAL': 50,
        'ERROR': 40,
        'WARNING': 30,
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0
    }
    return log_depth[key] if key in log_depth else 20

def fail_callback(context):
    uid = context["dag"].dag_id.split("_")[0]
    fail_file = glob.glob(os.path.join(folder_running, uid+"*"))
    if len(fail_file) != 1:
        raise Exception("Must be one failed file:{0}".format(fail_file))
    shutil.move(fail_file[0], folder_fail)
    print("Failed uid: {0} file: {1}".format(uid, fail_file[0]))


def gen_uid (job_file):
    with open(job_file, 'r') as f:
        job = yaml.safe_load(f)
    return job.get("uid", '.'.join(job_file.split("/")[-1].split('.')[0:-1]))


def gen_dag_id (workflow_file, job_file):
    return ".".join(workflow_file.split("/")[-1].split(".")[0:-1]) + "-" + gen_uid(
        job_file) + "-" + datetime.fromtimestamp(os.path.getctime(job_file)).isoformat().replace(':', '-')


def make_dag(job_file, workflow_file):
    with open(job_file, 'r') as f:
        job = yaml.safe_load(f)
    dag_id = gen_dag_id(workflow_file, job_file)

    start_day = datetime.fromtimestamp(os.path.getctime(job_file))

    basedir = os.path.abspath(os.path.dirname(job_file))

    output_folder = job.get('output_folder', os.path.join(conf_get_default('biowardrobe', 'OUTPUT_FOLDER', os.getcwd()), dag_id))
    output_folder = output_folder if os.path.isabs(output_folder) else os.path.normpath(os.path.join(basedir, output_folder))

    if not os.path.exists(output_folder):
        os.makedirs(output_folder, 0777)

    tmp_folder = job.get('tmp_folder', conf_get_default('biowardrobe', 'TMP_FOLDER', tempfile.mkdtemp()))
    tmp_folder = tmp_folder if os.path.isabs(tmp_folder) else os.path.normpath(os.path.join(basedir, tmp_folder))

    if not os.path.exists(tmp_folder):
        os.makedirs(tmp_folder, 0777)

    owner = job.get('author', 'biowardrobe')

    default_args = {
        'owner': owner,
        'start_date': start_day,
        'email_on_failure': False,
        'email_on_retry': False,
        'end_date': None, # Open ended schedule
        'on_failure_callback': fail_callback,
        'output_folder': output_folder,
        'tmp_folder': tmp_folder,

        'print_deps': False,
        'print_pre': False,
        'print_rdf': False,
        'print_dot': False,
        'relative_deps': False,
        'tmp_outdir_prefix': os.path.join(tmp_folder, 'cwl_outdir_'),
        'use_container': True,
        'preserve_environment': ["PATH"],
        'preserve_entire_environment': False,
        "rm_container": True,
        'tmpdir_prefix': os.path.join(tmp_folder, 'cwl_tmp_'),
        'print_input_deps': False,
        'cachedir': None,
        'rm_tmpdir': True,
        'move_outputs': 'move',
        'enable_pull': True,
        'eval_timeout': 20,
        'quiet': False,
        # # 'debug': False,   # TODO Check if it influence somehow. It shouldn't
        'version': False,
        'enable_dev': False,
        'enable_ext': False,
        'strict': conf_get_default('biowardrobe', 'STRICT', 'False').lower() in ['true', '1', 't', 'y', 'yes'],
        'rdf_serializer': None,
        'basedir': basedir,
        'tool_help': False,
        # 'workflow': None,
        # 'job_order': None,
        'pack': False,
        'on_error': 'continue',
        'relax_path_checks': False,
        'validate': False,
        'compute_checksum': True,
        "no_match_user" : False
    }

    dag = CWLDAG(
        dag_id=dag_id,
        cwl_workflow = workflow_file,
        schedule_interval = '@once',
        default_args=default_args)
    dag.create()
    dag.assign_job_dispatcher(JobDispatcher(task_id="read", read_file=job_file, dag=dag))
    dag.assign_job_cleanup(JobCleanup(task_id="cleanup",
                                      outputs=dag.get_output_list(),
                                      rm_files=[job_file],
                                      rm_files_dest_folder=os.path.join('/'.join(job_file.split('/')[0:-2]), 'success'),
                                      dag=dag))
    globals()[dag_id] = dag


def find_workflow(job_filename):
    workflows_folder = conf.get('biowardrobe', 'CWL_WORKFLOWS')
    job_keyname = "-".join(os.path.basename(job_filename).split("-")[0:-1])
    workflow_filenames = []
    for root, dirs, files in os.walk(workflows_folder):
        workflow_filenames.extend([os.path.join(root, filename) for filename in files if filename == job_keyname+".cwl"])
    if len(workflow_filenames) > 0:
        return min(workflow_filenames, key=os.path.getctime)
    else:
        raise ValueError


def get_jobs_folder_structure(monitor_folder):
    jobs = []
    for root, dirs, files in os.walk(monitor_folder):
        if 'new' in dirs:
            job_rec = { "new": os.path.join(root, "new"),
                        "running": os.path.join(root, "running"),
                        "fail": os.path.join(root, "fail"),
                        "success": os.path.join(root, "success")}
            for key,value in job_rec.iteritems():
                if not os.path.exists(value):
                    raise ValueError("Failed to find {}".format(value))
            jobs.append(job_rec)
    return jobs


logging.getLogger('cwltool').setLevel(eval_log_level(conf_get_default('biowardrobe', 'LOG_LEVEL', 'INFO').upper()))

max_jobs_to_run = get_max_jobs_to_run()
monitor_folder = conf.get('biowardrobe', 'CWL_JOBS')

jobs_list = get_jobs_folder_structure (monitor_folder)

tot_files_run = len(get_only_files(jobs_list, key="running"))
tot_files_new = len(get_only_files(jobs_list, key="new"))

# add new jobs into running
if tot_files_run < max_jobs_to_run and tot_files_new > 0:
    for i in range(min(max_jobs_to_run - tot_files_run, tot_files_new)):
        oldest = min(get_only_files(jobs_list, key="new"), key=os.path.getctime)
        print "mv {0} {1}".format (oldest, os.path.join('/'.join(oldest.split('/')[0:-2]), 'running'))
        shutil.move(oldest, os.path.join('/'.join(oldest.split('/')[0:-2]), 'running'))


for fn in get_only_files(jobs_list, key="running"):
    try:
        make_dag(fn, find_workflow(fn))
    except ValueError:
        shutil.move(fn, os.path.join('/'.join(fn.split('/')[0:-2]), 'fail'))

