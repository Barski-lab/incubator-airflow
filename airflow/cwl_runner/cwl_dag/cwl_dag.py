#!/usr/bin/env python
import sys

from airflow.configuration import conf
from datetime import datetime
from biow_cwl_runner.cwldag import CWLDAG
from biow_cwl_runner.jobdispatcher import JobDispatcher
from biow_cwl_runner.jobcleanup import JobCleanup
import glob
import os
import shutil
import ruamel.yaml as yaml
import logging
import tempfile
from biow_cwl_runner.cwlutils import conf_get_default, get_only_file


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
    return job.get("uid", '.'.join(job_file.split("/")[-1].split('.')[0:-1]).split("-")[-1])


def gen_dag_id (workflow_file, job_file):
    return ".".join(workflow_file.split("/")[-1].split(".")[0:-1]) + "-" + gen_uid(job_file)


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
    dag.assign_job_cleanup(JobCleanup(task_id="cleanup", outputs=dag.get_output_list(), rm_files=[job_file], rm_files_dest_folder=folder_success, dag=dag))
    globals()[dag_id] = dag


def find_workflow(job_filename):
    workflows_folder = conf.get('biowardrobe', 'CWL_WORKFLOWS')
    job_keyname = "-".join(os.path.basename(job_filename).split("-")[0:-1])
    workflow_filenames = glob.glob(os.path.join(workflows_folder,job_keyname+".cwl"))
    if len(workflow_filenames) > 0:
        return workflow_filenames[0]
    else:
        raise ValueError


logging.getLogger('cwltool').setLevel(eval_log_level(conf_get_default('biowardrobe', 'LOG_LEVEL', 'INFO').upper()))

max_jobs_to_run = get_max_jobs_to_run()
monitor_folder = conf.get('biowardrobe', 'CWL_JOBS')

folder_new = os.path.join(monitor_folder, "new")
folder_running = os.path.join(monitor_folder, "running")
folder_fail = os.path.join(monitor_folder, "fail")
folder_success = os.path.join(monitor_folder, "success")


if not os.path.exists(folder_running):
    os.mkdir(folder_running, 0777)
if not os.path.exists(folder_fail):
    os.mkdir(folder_fail, 0777)
if not os.path.exists(folder_success):
    os.mkdir(folder_success, 0777)


tot_files_run = len(get_only_file(folder_running))
tot_files_new = len(get_only_file(folder_new))

# add new jobs into running
if tot_files_run < max_jobs_to_run and tot_files_new > 0:
    for i in range(min(max_jobs_to_run - tot_files_run, tot_files_new)):
        oldest = max(get_only_file(folder_new), key=os.path.getctime)
        print "mv {0} {1}".format (oldest, folder_running)
        shutil.move(oldest, folder_running)


for fn in get_only_file(folder_running):
    try:
        make_dag(fn, find_workflow(fn))
    except ValueError:
        shutil.move(fn, folder_fail)

