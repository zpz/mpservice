import asyncio
import contextlib
import time
from logging import getLogger
from time import sleep

import httpcore
import httpx
import pytest
from mpservice.http import start_server, stop_server
from mpservice.mpserver import AsyncServer, ThreadServlet, Worker
from mpservice.multiprocessing import Process, Queue
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient
from zpz.logging import config_logger, unuse_console_handler

unuse_console_handler()
config_logger(with_thread_name=True, with_process_name=True)

logger = getLogger('test')


HOST = '0.0.0.0'
SHUTDOWN_MSG = 'server shutdown'


def make_app():
    async def simple1(request):
        return PlainTextResponse('1', status_code=201)

    async def simple2(request):
        return JSONResponse({'result': 2}, status_code=202)

    a = Starlette()
    a.add_route('/simple1', simple1, ['GET', 'POST'])
    a.add_route('/simple2', simple2, ['GET', 'POST'])
    return a


def _run_app(port):
    app = make_app()

    async def shutdown(request):
        await stop_server()
        return PlainTextResponse(SHUTDOWN_MSG, status_code=200)

    app.add_route('/shutdown', shutdown, ['POST'])
    start_server(app, host=HOST, port=port)


def test_testclient():
    with TestClient(make_app()) as client:
        response = client.get('/simple1')
        assert response.status_code == 201
        assert response.text == '1'

        response = client.get('/simple2')
        assert response.status_code == 202
        assert response.json() == {'result': 2}


# This one failed in `./run-tests` but succeeded when run
# interactively within a container.
@pytest.mark.asyncio
async def test_mp():
    process = Process(target=_run_app, args=(8002,))
    process.start()
    await asyncio.sleep(2)

    # Below, using a sync client wouldn't work.
    # Don't know why.

    url = 'http://0.0.0.0:8002'

    async with httpx.AsyncClient() as client:
        response = await client.get(url + '/simple1')
        # Occasionally, this fails during release test.

        assert response.status_code == 201
        assert response.text == '1'
        response = await client.get(url + '/simple2')
        assert response.status_code == 202
        assert response.json() == {'result': 2}

        response = await client.post(url + '/shutdown')
        assert response.status_code == 200
        assert response.text == SHUTDOWN_MSG

        time.sleep(1)

        with pytest.raises(httpx.ConnectError):
            response = await client.get(url + '/simple1')
            assert response.status_code == 201

    process.join()


class MyWorker(Worker):
    def call(self, x):
        return x * 2


class MyServer(AsyncServer):
    def __init__(self):
        super().__init__(ThreadServlet(MyWorker))


async def double(request):
    data = await request.json()
    x = data['x']
    y = await request.state.model.call(x)
    return JSONResponse({'x': x, 'y': y}, status_code=200)


async def shutdown(request):
    await stop_server()
    return PlainTextResponse(SHUTDOWN_MSG, status_code=200)


@contextlib.asynccontextmanager
async def lifespan(app):
    worker_context = app.worker_context
    model = MyServer()
    async with model:
        worker_context['q_ack'].put(worker_context['worker_idx'])
        yield {'model': model}


app = Starlette(
    lifespan=lifespan,
    routes=[
        Route('/double', double, methods=['POST']),
        Route('/shutdown', shutdown, methods=['POST']),
    ],
)


def test_server():
    port = 8002
    acks = Queue()
    p = Process(
        target=start_server,
        kwargs={
            'app': 'test_http:app',
            'workers': 4,
            'worker_contexts': [{'worker_idx': i, 'q_ack': acks} for i in range(4)],
            'port': port,
            'log_config': None,
        },
    )
    p.start()

    signals = []
    for _ in range(4):
        signals.append(acks.get())
    assert sorted(signals) == [0, 1, 2, 3]

    url = f'http://0.0.0.0:{port}'

    with httpx.Client(timeout=0.1) as client:
        retry = 0
        while True:
            try:
                response = client.post(url + '/double', json={'x': 8})
                break
            except (
                httpx.ConnectError,
                httpcore.ConnectError,
                ConnectionRefusedError,
            ) as e:
                retry += 1
                if retry == 5:
                    raise
                print('retry', retry, ' on error:', e)
                time.sleep(0.2)

        assert response.status_code == 200
        assert response.json() == {'x': 8, 'y': 16}

        response = client.post(url + '/double', json={'x': 80})
        assert response.status_code == 200
        assert response.json() == {'x': 80, 'y': 160}

        response = client.post(url + '/shutdown')
        assert response.status_code == 200
        assert response.text == SHUTDOWN_MSG

        sleep(1)

        with pytest.raises(httpx.ConnectError):
            response = client.post(url + '/double', json={'x': 6})
            assert response.status_code == 201

    p.join()
