import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, JSONResponse
from starlette.testclient import TestClient
from mpservice.http_server import SHUTDOWN_MSG, SHUTDOWN_STATUS
from mpservice.http_server import stop_starlette_server
from mpservice.http_server import run_local_app


@pytest.fixture()
def app():

    async def simple1(request):
        return PlainTextResponse('1', status_code=201)

    async def simple2(request):
        return JSONResponse({'result': 2}, status_code=202)

    a = Starlette()
    a.add_route('/simple1', simple1, ['GET', 'POST'])
    a.add_route('/simple2', simple2, ['GET', 'POST'])
    a.add_route('/stop', stop_starlette_server, ['POST'])
    return a


@pytest.mark.asyncio
async def test_run(app):
    url = 'http://127.0.0.1:8080'
    async with run_local_app(app, port=8080, limit_max_requests=2):
        async with httpx.AsyncClient() as client:
            response = await client.get(url + '/simple1')
            assert response.status_code == 201
            assert response.text == '1'
            response = await client.get(url + '/simple2')
            assert response.status_code == 202
            assert response.json() == {'result': 2}


@pytest.mark.asyncio
async def test_shutdown(app):
    url = 'http://127.0.0.1:8080'
    async with run_local_app(app, port=8080) as server:
        print('server state tasks:', server.server_state.tasks)
        async with httpx.AsyncClient() as client:
            response = await client.get(url + '/simple1')
            assert response.status_code == 201
            print('server state tasks:', server.server_state.tasks)

            response = await client.get(url + '/simple2')
            assert response.status_code == 202
            print('server state tasks:', server.server_state.tasks)

            response = await client.post(url + '/stop')
            assert response.status_code == 200
            assert response.text == SHUTDOWN_MSG
            print('server state tasks:', server.server_state.tasks)

            with pytest.raises(httpx.ConnectError):
                response = await client.get(url + '/simple1')
                assert response.status_code == 201


def test_testclient(app):
    with TestClient(app) as client:
        response = client.get('/simple1')
        assert response.status_code == 201
        assert response.text == '1'

        response = client.get('/simple2')
        assert response.status_code == 202
        assert response.json() == {'result': 2}

        # This approach does not run middleware,
        # hence the 'shutdown' mechanism does not have an effect.

        response = client.post('/stop')
        assert response.status_code == SHUTDOWN_STATUS
        assert response.text == SHUTDOWN_MSG

        response = client.get('/simple1')
        assert response.status_code == 201


# This failed in `./run-tests` and succeeded when run
# interactively within a container.
# @pytest.mark.asyncio
# async def test_mp(app):
#     process = mp.Process(
#         target=run_app, args=(app,), kwargs={'port': 8080}
#     )
#     process.start()

#     url = 'http://127.0.0.1:8080'
#     async with httpx.AsyncClient() as client:
#         response = await client.get(url + '/simple1')
#         assert response.status_code == 201
#         assert response.text == '1'
#         response = await client.get(url + '/simple2')
#         assert response.status_code == 202
#         assert response.json() == {'result': 2}

#         response = await client.post(url + '/stop')
#         assert response.status_code == 200
#         assert response.text == SHUTDOWN_MSG

#         with pytest.raises(httpx.ConnectError):
#             response = await client.get(url + '/simple1')
#             assert response.status_code == 201

#         process.join()
