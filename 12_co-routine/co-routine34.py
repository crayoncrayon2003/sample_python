import asyncio
import time

async def myTask1(wait):
    while True:
        await asyncio.sleep(wait)
        print("wait:"+str(wait))

async def myTask2():
    """
    Listener for quitting the sample
    """
    while True:
        selection = input("Press Q to quit\n")
        if selection == "Q" or selection == "q":
            print("Quitting...")
            break

async def main():
    loop = asyncio.get_event_loop()
    user_finished = loop.run_in_executor(None, myTask2)

    myTasks = {
        myTask1(3),
        myTask1(4),
    }
    result = loop.run_until_complete(asyncio.gather(*myTasks))
    await user_finished


if __name__ == '__main__':
    main()
