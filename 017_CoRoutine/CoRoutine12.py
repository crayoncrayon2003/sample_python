import asyncio
import time

async def one_print():
    await asyncio.sleep(3)
    print(1)

async def two_print():
    print(2)

async def main():
    task1 = asyncio.create_task(one_print())
    task2 = asyncio.create_task(two_print())
    await task1
    await task2

if __name__ == '__main__':
    # This example does achieve parallel processing
    asyncio.run(main())
