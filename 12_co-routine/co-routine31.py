import asyncio
import time

async def stdin_listener():
    while True:
        selection = input("Press Q to quit\n")
        if selection == "Q" or selection == "q":
            print("Quitting...")
            break

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(stdin_listener())
    loop.close()

if __name__ == '__main__':
    main()
