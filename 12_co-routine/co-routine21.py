import asyncio
import time

async def myTask(wait):
    await asyncio.sleep(wait)
    print("wait:"+str(wait))
    return wait

def main():
    loop = asyncio.get_event_loop()
    task1 = asyncio.ensure_future(myTask(3))
    task2 = asyncio.ensure_future(myTask(5))
    task3 = asyncio.ensure_future(myTask(2))
    loop.run_forever()
    loop.close()

if __name__ == '__main__':
    # This example does achieve parallel processing
    main()
