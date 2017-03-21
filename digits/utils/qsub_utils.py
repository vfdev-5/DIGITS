# qsub_utils

import os
import subprocess
import platform
import logging

PBS_CONFIGURATION = {}
logger = logging.getLogger('qsub_utils')


def setup_configuration(nodes='', stdout='', stderr=''):
    """
    Method to setup PBS configuration
    :param nodes: specify nodes with list of features. `-l nodes={nodes}`
    :param stdout: specify stdout file. `-o {stdout}`
    :param stderr: specify stderr file. `-e {stderr}`
    """
    global PBS_CONFIGURATION
    conf = "#PBS"
    if len(nodes) > 0:
        PBS_CONFIGURATION['nodes'] = nodes
        conf += " -l nodes=%s" % nodes
    if len(stdout) > 0:
        PBS_CONFIGURATION['stdout'] = stdout
        conf += " -o %s" % stdout
    if len(stderr) > 0:
        PBS_CONFIGURATION['stderr'] = stderr
        conf += " -e %s" % stderr

    PBS_CONFIGURATION['cmd'] = conf


def write_launch_file(cmd_str, name, cwd='', env=''):
    """
    Method to write a PBS launch file for qsub

    :param cmd_str: command string, e.g "python -c \'import sys; sys.print\'"
    :param name: name of the job
    :param cwd: current working directory
    :param env: environmanet string, e.g. "export PATH=$PATH:/path/to/bin"
    """
    assert len(cmd_str) > 0, "Job command can't be empty"
    assert len(name) > 0, "Job name can't be empty"
    assert len(PBS_CONFIGURATION) > 0, "PBS configuration is not setup. Call `setup_configuration` before."
    filename = os.path.join(cwd, "job_%s.launch" % name)
    with open(filename, 'w') as w:
        conf = PBS_CONFIGURATION['cmd']
        conf += " -N %s" % name
        if len(cwd) > 0:
            conf += " -d %s" % cwd
        w.write(conf + '\n')
        if len(env) > 0:
            w.write(env + '\n')
        w.write(cmd_str)

    return filename


def submit_job(cmd, name, cwd='', env=''):
    """
    Method to submit a job writing a launch file and using qsub
    `qsub job_{name}.launch`

    :param cmd: list of commands, e.g. ['python', '-c', '\"import sys; print sys.path\"']
    :param name: name of the job
    :param cwd: current working directory
    :param env: environmanet string, e.g. "export PATH=$PATH:/path/to/bin"
    """
    if ' ' in name:
        name = name.replace(' ', '_')
    filename = write_launch_file(' '.join(cmd), name, cwd, env)
    program = ['qsub', filename]
    process = subprocess.Popen(program,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               close_fds=False if platform.system() == 'Windows' else True)
    process.wait()
    job_id = process.stdout.read()
    assert job_id is not None and len(job_id) > 0, "Failed to fetch job id from qsub"

    job_info = dict()
    job_info['id'] = job_id.replace('\n', '')
    job_info['launch_filename'] = filename
    job_info['name'] = name
    job_info['cwd'] = cwd
    job_info['stdout_filename'] = os.path.join(PBS_CONFIGURATION['stdout'], "%s.o%s" % (name, _get_id(job_id)))
    job_info['stderr_filename'] = os.path.join(PBS_CONFIGURATION['stderr'], "%s.e%s" % (name, _get_id(job_id)))

    return job_info


def delete_job(job_id):
    if not job_is_running(job_id):
        logger.warn("Job '%s' is not running. Can not delete job" % job_id)
        return False
    _id = _get_id(job_id)
    program = ['qdel', _id]
    process = subprocess.Popen(program,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               close_fds=False if platform.system() == 'Windows' else True)
    returncode = process.wait()
    return returncode == 0


def _get_id(job_id):
    _id = job_id.split('.')[0]
    return _id


def get_stats(job_id):
    _id = _get_id(job_id)
    program = ['qstat', '-f', _id]
    process = subprocess.Popen(program,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               close_fds=False if platform.system() == 'Windows' else True)
    process.wait()
    out = process.stdout.read()
    out = out.split('\n')
    stats = {}
    if len(out) > 0:
        for line in out:
            kv = line.split(' = ')
            if len(kv) > 1:
                stats[kv[0].strip()] = kv[1].strip()
    return stats


def job_is_running(job_id):
    stats = get_stats(job_id)
    if len(stats) > 0:
        """
        the job states
            E -    Job is exiting after having run.
            H -    Job is held.
            Q -    job is queued, eligable to run or routed.
            R -    job is running.
            T -    job is being moved to new location.
            W -    job is waiting for its execution time (-a option) to be reached.
            S -    (Unicos only) job is suspend.
        """
        if stats['job_state'] in ['R', 'Q', 'H', 'T', 'W']:
            return True
    else:
        return False


def get_stdout(job_info):
    filename = job_info['stdout_filename']
    if not os.path.exists(filename):
        logger.warn("Stdout filename %s' is not found" % filename)
        return None
    out = []
    with open(filename, 'r') as r:
        out.append(r.readline())
    return out


def get_stderr(job_info):
    filename = job_info['stderr_filename']
    if not os.path.exists(filename):
        logger.warn("Stdout filename %s' is not found" % filename)
        return None
    out = []
    with open(filename, 'r') as r:
        out.append(r.readline())
    return out
