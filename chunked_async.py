#!/usr/bin/env python3

'''
Copyright (c) 2024 Godwin Peter .O

Licensed under the MIT License
you may not use this file except in compliance with the License.
    https://opensource.org/license/mit
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Godwin peter .O (me@godwin.dev)
Created At: Saturday, 7th Dec 2024
Modified By: Godwin peter .O
Modified At: Sat Dec 07 2024
'''

import asyncio
from random import uniform

chunk_count: int = 5
total_item: int = 43
task_delay: float = 1.25
max_chunk_task_delay: float = 7.5

def get_chunks(items: list[int], step: int) -> list[list[int]]:
    """
    Divide a list into smaller chunks of a given size.

    Args:
        items (list[int]): The list of items to chunk.
        step (int): The size of each chunk.

    Returns:
        list[list[int]]: A list of chunks.
    """
    
    return [items[i:i + step] for i in range(0, len(items), step)]

async def run_chunk(item: int) -> None:
    """
    Simulate processing of an individual item with a delay.

    Args:
        item (int): The item to process.

    Returns:
        None
    """
    await asyncio.sleep(task_delay)
    print("Executing item => {0}".format(item))
    
async def run_async_chunks(chunks: list[int]) -> None:
    """
    Asynchronously process a list of items (chunk) with a delay.

    Args:
        chunks (list[int]): The list of items in the current chunk.

    Returns:
        None
    """
    
    # A random float for simulating a delay in seconds for demonstration only
    delay: float = uniform(0.1, max_chunk_task_delay)
    
    print(f"Processing chunks {0} to {1}, with {delay:.2f}s delay".format(chunks[0], chunks[len(chunks) - 1]))
    
    await asyncio.sleep(delay)
    await asyncio.gather(*(run_chunk(item) for item in chunks))

async def main() -> None:
    """
    Main function to orchestrate asynchronous chunk processing.

    - Divides the list of items into chunks.
    - Processes each chunk asynchronously with random delays.

    Returns:
        None
    """
    
    items: list[int] = list(range(0, total_item))
    chunked_items = list(get_chunks(items, chunk_count))
    
    # Main task execution
    tasks = [run_async_chunks(i) for i in chunked_items]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
