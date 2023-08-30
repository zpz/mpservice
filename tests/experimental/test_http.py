import contextlib
import os
from time import sleep
from types import SimpleNamespace

import httpx
import pytest
from mpservice.experimental.http import start_server, stop_server
from mpservice.mpserver import AsyncServer, ThreadServlet, Worker
from mpservice.multiprocessing import Process
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse

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
    y = await context.model.call(x)
    return JSONResponse({'x': x, 'y': y}, status_code=200)


async def shutdown(request):
    await stop_server()
    return PlainTextResponse('shutdown', status_code=200)


@contextlib.asynccontextmanager
async def lifespan(app):
    print('worker index:', os.environ['UVICORN_WORKER_IDX'])
    model = MyServer()
    context.model = model
    async with model:
        yield


app = Starlette(lifespan=lifespan)
app.add_route('/double', double, ['POST'])
app.add_route('/shutdown', shutdown, ['POST'])


def test_server():
    port = 8002
    p = Process(
        target=start_server,
        kwargs={'app': 'test_http:app', 'workers': 4, 'port': port, 'log_config': None},
    )
    p.start()
    sleep(1)

    url = f'http://0.0.0.0:{port}'

    with httpx.Client() as client:
        response = client.post(url + '/double', json={'x': 8})
        assert response.status_code == 200
        assert response.json() == {'x': 8, 'y': 16}

        response = client.post(url + '/shutdown')
        assert response.status_code == 200
        assert response.text == 'shutdown'

        sleep(1)

        with pytest.raises(httpx.ConnectError):
            response = client.post(url + '/double', json={'x': 6})
            assert response.status_code == 201

    p.join()
