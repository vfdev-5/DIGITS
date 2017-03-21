from __future__ import absolute_import

from .local_task import LocalTask
from .grid_engine_task import GridEngineTask
from .utils import qsub_utils


Task = None


def setup_backend(backend_type='local', **kwargs):
    global Task
    if backend_type == 'local':
        Task = LocalTask
    elif backend_type == 'grid_engine':
        Task = GridEngineTask
        qsub_utils.setup_configuration(**kwargs)
    else:
        raise Exception('Task backend type \'%s\' is unknown' % backend_type)
