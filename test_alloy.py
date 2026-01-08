import eth_lake
import asyncio
import os

RPC_URL = "https://eth.llamarpc.com" 

async def main():
    print(f"Connecting to {RPC_URL}...")
    try:
        block_num = await eth_lake.get_latest_block(RPC_URL)
        print(f"[SUCCESS!]Latest Block Number: {block_num}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())