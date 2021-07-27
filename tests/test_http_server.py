import multiprocessing as mp
import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, JSONResponse
from mpservice.http_server import SHUTDOWN_MSG, run_local_app, run_app


@pytest.fixture()
def app():

    async def simple1(request):
        return PlainTextResponse('1', status_code=201)

    async def simple2(request):
        return JSONResponse({'result': 2}, status_code=202)

    a = Starlette()
    a.add_route('/simple1', simple1, ['GET', 'POST'])
    a.add_route('/simple2', simple2, ['GET', 'POST'])
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

            response = await client.get(url + '/stop')
            assert response.status_code == 200
            assert response.text == SHUTDOWN_MSG
            print('server state tasks:', server.server_state.tasks)

            with pytest.raises(httpx.ConnectError):
                response = await client.get(url + '/simple1')
                assert response.status_code == 201


@pytest.mark.asyncio
async def test_mp(app):
    process = mp.Process(
        target=run_app, args=(app,), kwargs={'port': 8080}
    )
    process.start()

    url = 'http://127.0.0.1:8080'
    async with httpx.AsyncClient() as client:
        response = await client.get(url + '/simple1')
        assert response.status_code == 201
        assert response.text == '1'
        response = await client.get(url + '/simple2')
        assert response.status_code == 202
        assert response.json() == {'result': 2}

        response = await client.get(url + '/stop')
        assert response.status_code == 200
        assert response.text == SHUTDOWN_MSG

        # TODO: 'Future exception was never retrieved' error
        # during `make-release`.
        # with pytest.raises(httpx.ConnectError):
        #     response = await client.get(url + '/simple1')
        #     assert response.status_code == 201

        process.join()
