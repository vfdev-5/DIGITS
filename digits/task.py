

from local_task import LocalTask
from grid_engine_task import GridEngineTask

Task = None


def setup_backend(backend_type='local'):
    global Task
    if backend_type == 'local':
        Task = LocalTask
    elif backend_type == 'grid_engine':
        Task = GridEngineTask
    else:
        raise Exception('Task backend type \'%s\' is unknown' % backend_type)
