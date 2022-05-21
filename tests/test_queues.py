import multiprocessing
import time
from queue import Empty
from mpservice._queues import Unique
from mpservice.mpserver import BatchQueue
from mpservice.streamer import Streamer

import pytest


@pytest.fixture(params=[None, 'spawn'])
def mp(request):
    return multiprocessing.get_context(request.param)



def test_unique():
    q = Unique()
    q.put(3)
    assert q.get() == 3
