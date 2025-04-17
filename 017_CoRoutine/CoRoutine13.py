import asyncio
import time

async def myTask(wait):
    await asyncio.sleep(wait)
    print("wait:"+str(wait))
    return wait

async def main():
    task1 = asyncio.create_task(myTask(3))
    task2 = asyncio.create_task(myTask(5))
    task3 = asyncio.create_task(myTask(2))
    result1 = await task1
    result2 = await task2
    result3 = await task3
    print(result1, result2, result3)

if __name__ == '__main__':
    # This example does achieve parallel processing
    asyncio.run(main())
