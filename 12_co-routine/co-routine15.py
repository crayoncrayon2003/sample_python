import asyncio
import time

async def myTask(wait):
    await asyncio.sleep(wait)
    print("wait:"+str(wait))
    return wait

def main():
    myTasks = {
        myTask(3),
        myTask(5),
        myTask(2)
    }
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(asyncio.gather(*myTasks))
    print(result)

if __name__ == '__main__':
    # This example does achieve parallel processing
    main()
