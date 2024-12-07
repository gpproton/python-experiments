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
    return [items[i:i + step] for i in range(0, len(items), step)]

async def run_chunk(item: int) -> None:
        await asyncio.sleep(task_delay)
        print("Executing item => {0}".format(item))
    
async def run_async_chunks(chunks: list[int]) -> None:
    # A random float for simulating a delay in seconds for demonstration only
    delay: float = uniform(0.1, max_chunk_task_delay)
    
    print(f"Processing chunks {0} to {1}, with {delay:.2f}s delay".format(chunks[0], chunks[len(chunks) - 1]))
    
    await asyncio.sleep(delay)
    await asyncio.gather(*(run_chunk(item) for item in chunks))

async def main() -> None:
    items: list[int] = list(range(0, total_item))
    chunked_items = list(get_chunks(items, chunk_count))
    
    # Main task execution
    tasks = [run_async_chunks(i) for i in chunked_items]
    await asyncio.gather(*tasks)


asyncio.run(main())

