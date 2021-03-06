# Copyright (c) 2014-2017, NVIDIA CORPORATION.  All rights reserved.
from __future__ import absolute_import

import os
import sys
import time
import signal
from .status import Status
from .local_task import LocalTask
from .import utils


class GridEngineTask(LocalTask):
    """
    Base class for Tasks executed on a Grid Engine (e.g. SGE)
    """

    def __init__(self, **kwargs):
        super(GridEngineTask, self).__init__(**kwargs)
        self.job_info = None

    def run(self, resources):
        """
        Execute the task

        Arguments:
        resources -- the resources assigned by the scheduler for this task
        """
        # return False
        self.before_run()

        env = os.environ.copy()
        args = self.task_arguments(resources, env)
        if not args:
             self.logger.error('Could not create the arguments')
             self.status = Status.ERROR
             return False
        # Convert them all to strings
        args = [str(x) for x in args]

        self.logger.info('%s task started.' % self.name())
        self.status = Status.RUN
        print "Status: ", self.status

        unrecognized_output = []
        # https://docs.python.org/2/library/subprocess.html#converting-argument-sequence
        self.logger.info('Task subprocess args: "{}"'.format(args))        
        
        self.p, self.job_info = utils.qsub_utils.submit_job(cmd=args, name=self.name(), cwd=self.job_dir)
        
        try:
            sigterm_time = None  # When was the SIGTERM signal sent
            sigterm_timeout = 2  # When should the SIGKILL signal be sent
            while self.p.poll() is None:
                for line in utils.nonblocking_readlines(self.p.stdout):
                    
                    if self.aborted.is_set():
                        if sigterm_time is None:
                            # Attempt graceful shutdown
                            self.p.send_signal(signal.SIGTERM)
                            utils.qsub_utils.delete_job(self.job_info['id'])
                            sigterm_time = time.time()
                            self.status = Status.ABORT
                        break

                    if line is not None:
                        # Remove whitespace
                        line = line.strip()

                    if line:
                        if not self.process_output(line):
                            self.logger.warning('%s unrecognized output: %s' % (self.name(), line.strip()))
                            unrecognized_output.append(line)
                    else:
                        time.sleep(0.05)
                if sigterm_time is not None and (time.time() - sigterm_time > sigterm_timeout):
                    self.p.send_signal(signal.SIGKILL)
                    utils.qsub_utils.delete_job(self.job_info['id'])
                    self.logger.warning('Sent SIGKILL to task "%s"' % self.name())
                    time.sleep(0.1)
                time.sleep(0.01)
        except:
            self.p.terminate()
            utils.qsub_utils.delete_job(self.job_info['id'])
            self.after_run()
            raise
    
        print "After run"
        self.after_run()

        if self.status != Status.RUN:
            return False
        elif self.p.returncode != 0:
            self.logger.error('%s task failed with error code %d' % (self.name(), self.p.returncode))
            if self.exception is None:
                self.exception = 'error code %d' % self.p.returncode
                if unrecognized_output:
                    if self.traceback is None:
                        self.traceback = '\n'.join(unrecognized_output)
                    else:
                        self.traceback = self.traceback + ('\n'.join(unrecognized_output))
            self.after_runtime_error()
            self.status = Status.ERROR
            return False
        else:
            self.logger.info('%s task completed.' % self.name())
            self.status = Status.DONE
            return True

