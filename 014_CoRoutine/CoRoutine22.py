import asyncio
import time

async def myTask(wait,loop):
    await asyncio.sleep(wait)
    print("wait:"+str(wait))
    loop.stop()
    return wait

def main():
    loop = asyncio.get_event_loop()
    task1 = asyncio.ensure_future(myTask(3,loop))
    task2 = asyncio.ensure_future(myTask(5,loop))
    task3 = asyncio.ensure_future(myTask(2,loop))
    loop.run_forever()

if __name__ == '__main__':
    # This example does achieve parallel processing
    main()
