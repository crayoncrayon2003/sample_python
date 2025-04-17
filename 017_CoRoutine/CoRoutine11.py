import asyncio
import time

async def one_print():
    await asyncio.sleep(3)
    print(1)

async def two_print():
    print(2)

async def main():
    await one_print()
    await two_print()

if __name__ == '__main__':
    # "This example does not achieve parallel processing
    asyncio.run(main())