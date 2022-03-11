import asyncio
import multiprocessing as mp
import time

import httpx
import pytest
from starlette.responses import PlainTextResponse
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, JSONResponse
from starlette.testclient import TestClient

from mpservice.http_server import make_server


HOST = '0.0.0.0'
PORT = 8000
LOCALHOST = f'http://0.0.0.0:{PORT}'
#LOCALHOST = f'http://{HOST}:{PORT}'
SHUTDOWN_MSG = "server shutdown as requested"


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
    server = make_server(app, host=HOST, port=PORT)

    async def shutdown(request):
        server.should_exit = True
        return PlainTextResponse(SHUTDOWN_MSG)

    app.add_route('/shutdown', shutdown, ['POST'])

    service = asyncio.create_task(server.serve())
    await asyncio.sleep(1)

    url = LOCALHOST

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
        print('server state tasks:', server.server_state.tasks)

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


def _run_app():
    app = make_app()
    server = make_server(app, host=HOST, port=PORT)

    async def shutdown(request):
        server.should_exit = True
        return PlainTextResponse(SHUTDOWN_MSG, status_code=200)

    app.add_route('/shutdown', shutdown, ['POST'])
    server.run()


# This one failed in `./run-tests` and succeeded when run
# interactively within a container.
@pytest.mark.asyncio
async def test_mp():
    process = mp.Process(target=_run_app)
    process.start()
    await asyncio.sleep(1)

    # Below, using a sync client wouldn't work.
    # Don't know why.

    url = LOCALHOST
    async with httpx.AsyncClient() as client:
        response = await client.get(url + '/simple1')
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

