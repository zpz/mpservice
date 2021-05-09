import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, JSONResponse
from mpservice._http import make_server, SHUTDOWN_MSG, SHUTDOWN_RESPONSE


# Refer to tests in `uvicorn`.
@asynccontextmanager
async def run_server(server, sockets=None):
    concel_handle = asyncio.ensure_future(server.serve(sockets=sockets))
    await asyncio.sleep(0.1)
    try:
        yield server
    finally:
        await server.shutdown()
        concel_handle.cancel()


@pytest.fixture()
def app():

    async def simple1(request):
        return PlainTextResponse('1', status_code=201)

    async def simple2(request):
        return JSONResponse({'result': 2}, status_code=202)

    async def stop(request):
        return SHUTDOWN_RESPONSE

    a = Starlette()
    a.add_route('/simple1', simple1, ['GET', 'POST'])
    a.add_route('/simple2', simple2, ['GET', 'POST'])
    a.add_route('/stop', stop, ['GET', 'POST'])
    return a


@pytest.mark.asyncio
async def test_run(app):
    server = make_server(
        app, port=8080, limit_max_requests=2)
    url = 'http://127.0.0.1:8080'
    async with run_server(server):
        async with httpx.AsyncClient() as client:
            response = await client.get(url + '/simple1')
            assert response.status_code == 201
            response = await client.get(url + '/simple2')
            assert response.status_code == 202


@pytest.mark.asyncio
async def test_shutdown(app):
    server = make_server(app, port=8080)
    url = 'http://127.0.0.1:8080'
    print('server state tasks:', server.server_state.tasks)
    async with run_server(server):
        print('server state tasks:', server.server_state.tasks)
        async with httpx.AsyncClient() as client:
            response = await client.get(url + '/simple1')
            assert response.status_code == 201
            print('server state tasks:', server.server_state.tasks)

            response = await client.get(url + '/simple2')
            assert response.status_code == 202
            print('server state tasks:', server.server_state.tasks)

            response = await client.get(url + '/stop')
            assert response.status_code == 200
            assert response.text == SHUTDOWN_MSG
            print('server state tasks:', server.server_state.tasks)

            with pytest.raises(httpx.ConnectError):
                response = await client.get(url + '/simple1')
                assert response.status_code == 201
