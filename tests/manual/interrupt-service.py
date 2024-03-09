import asyncio
import random
import time
import contextlib
from multiprocessing import current_process
from threading import current_thread

from mpservice.mpserver import Worker, ProcessServlet, SequentialServlet, Server, AsyncServer
from mpservice.streamer import Stream
from mpservice.http import start_server, stop_server
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, JSONResponse
from starlette.routing import Route
from zpz.logging import config_logger


config_logger(level='debug', with_thread_name=True, with_process_name=True)


class Doubler(Worker):
    def call(self, x):
        time.sleep(random.uniform(0.001, 0.01))
        return x + x


class Tripler(Worker):
    def call(self, x):
        time.sleep(random.uniform(0.001, 0.01))
        return [v + v + v for v in x]



async def stop(request):
    await stop_server()
    return PlainTextResponse("server shutdown as requested", status_code=200)


@contextlib.asynccontextmanager
async def lifespan(app):
    server = AsyncServer(
        SequentialServlet(
            ProcessServlet(Doubler, cpus=[1, 2]),
            ProcessServlet(Tripler, cpus=[2, 3, 4], batch_size=100, batch_wait_time=0.01),
        ),
        capacity=256,
    )
    async with server as model:
        yield {'model': model}  # available in endpoint functions via `request.state`



def main():
    app = Starlette(lifespan=lifespan,
                    routes=[
                        Route('/stop', stop, methods=['POST']),
                    ])

    start_server(app)



if __name__ == '__main__':
    main()
