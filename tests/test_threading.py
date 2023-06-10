from mpservice.threading import Thread, InvalidStateError, wait, as_completed, FIRST_EXCEPTION
from time import sleep

import pytest


def worker(sleep_seconds):
    if sleep_seconds > 10:
        sleep(sleep_seconds * 0.1)
        raise ValueError(sleep_seconds)
    sleep(sleep_seconds)
    return sleep_seconds


def test_raise_exc():
    t = Thread(target=worker, args=(2,))
    t.start()
    sleep(0.1)
    t.raise_exc(ValueError)

    with pytest.raises(ValueError):
        t.result()

    assert not t.is_alive()

    with pytest.raises(InvalidStateError):
        t.raise_exc(ValueError)


def test_terminate():
    t = Thread(target=worker, args=(3,))
    t.start()
    sleep(0.1)
    t.terminate()
    assert not t.is_alive()
    assert t.result() is None
    assert t.exception() is None


def test_wait():
    threads = [
        Thread(target=worker, args=(x,))
        for x in (3, 2, 4)
    ]
    for t in threads:
        t.start()
    done, notdone = wait(threads, timeout=2.2)
    assert len(done) == 1
    assert done.pop() is threads[1]

def test_wait_exc():
    threads = [
        Thread(target=worker, args=(x,))
        for x in (3, 2, 13)
    ]
    for t in threads:
        t.start()
    done, notdone = wait(threads, return_when=FIRST_EXCEPTION)
    assert len(done) == 1
    assert done.pop() is threads[2]


    threads = [
        Thread(target=worker, args=(x,))
        for x in (3, 2, 25)
    ]
    for t in threads:
        t.start()
    done, notdone = wait(threads, return_when=FIRST_EXCEPTION)
    assert len(done) == 2
    assert notdone.pop() is threads[0]


def test_as_completed():
    threads = [
        Thread(target=worker, args=(x,))
        for x in (3, 2, 4)
    ]
    for t in threads:
        t.start()
    k = 0
    for t in as_completed(threads):
        if k == 0:
            assert t is threads[1]
        elif k == 1:
            assert t is threads[0]
        else:
            assert t is threads[2]
        k += 1
