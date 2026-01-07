import eth_lake
import asyncio
import time


async def main():
    print("Starting async operations...")

    # 1. Test the fast async function
    s = eth_lake.sum_as_string(3, 4)
    print(f"Result: {s}")

    # 2. Test the async sleep function
    print("Calling async sleep for 2 seconds...")
    start_time = time.time()
    await eth_lake.sleep_for(2)
    end_time = time.time()

    elapsed = end_time - start_time
    print(f"Slept for {elapsed:.2f} seconds")

    return None


if __name__ == "__main__":
    asyncio.run(main())
