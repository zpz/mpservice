import asyncio

async def main():
    loop = asyncio.get_running_loop()
    print(loop.set_default_executor())


if __name__ == '__main__':
    asyncio.run(main())
