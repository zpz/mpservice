import asyncio
import contextlib
import multiprocessing
import os
import time
from time import sleep
from types import SimpleNamespace

import httpcore
import httpx
import pytest
from mpservice.http import make_server, start_server, stop_server
from mpservice.mpserver import AsyncServer, ThreadServlet, Worker
from mpservice.multiprocessing import Process
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.testclient import TestClient
from starlette.routing import Route

HOST = '0.0.0.0'
SHUTDOWN_MSG = "server shutdown"


def make_app():
    async def simple1(request):
        return PlainTextResponse('1', status_code=201)

    async def simple2(request):
        return JSONResponse({'result': 2}, status_code=202)

    a = Starlette()
    a.add_route('/simple1', simple1, ['GET', 'POST'])
    a.add_route('/simple2', simple2, ['GET', 'POST'])
    return a


@pytest.mark.asyncio
async def test_shutdown():
    app = make_app()
    server = make_server(app, host=HOST, port=8001)

    async def shutdown(request):
        server.should_exit = True
        return PlainTextResponse(SHUTDOWN_MSG)

    app.add_route('/shutdown', shutdown, ['POST'])

    service = asyncio.create_task(server.serve())
    await asyncio.sleep(1)

    url = 'http://0.0.0.0:8001'

    async with httpx.AsyncClient() as client:
        response = await client.get(url + '/simple1')
        assert response.status_code == 201
        print('server state tasks:', server.server_state.tasks)

        response = await client.get(url + '/simple2')
        assert response.status_code == 202
        print('server state tasks:', server.server_state.tasks)

        response = await client.post(url + '/shutdown')
        assert response.status_code == 200
        assert response.text == SHUTDOWN_MSG
        # print('server state tasks:', server.server_state.tasks)

        await asyncio.sleep(1)

        with pytest.raises(httpx.ConnectError):
            response = await client.get(url + '/simple1')
            assert response.status_code == 201

    server.should_exit = True
    await service


def test_testclient():
    with TestClient(make_app()) as client:
        response = client.get('/simple1')
        assert response.status_code == 201
        assert response.text == '1'

        response = client.get('/simple2')
        assert response.status_code == 202
        assert response.json() == {'result': 2}


def _run_app(port):
    app = make_app()
    server = make_server(app, host=HOST, port=port)

    async def shutdown(request):
        server.should_exit = True
        return PlainTextResponse(SHUTDOWN_MSG, status_code=200)

    app.add_route('/shutdown', shutdown, ['POST'])
    server.run()


# This one failed in `./run-tests` but succeeded when run
# interactively within a container.
@pytest.mark.asyncio
async def test_mp():
    mp = multiprocessing.get_context('spawn')
    process = mp.Process(target=_run_app, args=(8002,))
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


# test new code #


# Gather global objects in this `context`.
context = SimpleNamespace()


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
    print('worker index:', os.environ['UVICORN_WORKER_IDX'])
    model = MyServer()
    async with model:
        yield {'model': model}


app = Starlette(lifespan=lifespan, routes=[
    Route('/double', double, methods=['POST']),
    Route('/shutdown', shutdown, methods=['POST']),
])


def test_server():
    port = 8002
    p = Process(
        target=start_server,
        kwargs={
            'app': 'test_http:app',
            'workers': 4,
            'port': port,
            'log_config': None,
        },
    )
    p.start()

    url = f'http://0.0.0.0:{port}'

    with httpx.Client() as client:
        retry = 0
        while True:
            try:
                response = client.post(url + '/double', json={'x': 8})
                break
            except (httpx.ConnectError, httpcore.ConnectError, ConnectionRefusedError) as e:
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
