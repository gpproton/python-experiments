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
from typing import TypedDict
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------
# Task definitions
# -----------------------------------------------

async def task_one(val: int):
    """
    Logs a message with the provided integer value.

    Args:
        val (int): An integer value used in the log message.

    Example:
        Task one-2 ticker...
    """
    logger.info("Task one-{0} ticker...".format(val))

async def task_two():
    """
    Logs a simple message indicating that the task is running.

    Example:
        Task two ticker...
    """
    logger.info("Task two ticker...")

TimerTask = TypedDict("TimerTask", {"interval": float, "task": any, "arg1": any })

timers = []
all_tasks: list[TimerTask] = [
    { "interval": 4, "task": task_one, "arg1": 2 },
    { "interval": 3, "task": task_two, "arg1": None  }
]

class Timer:
    """
    Represents a recurring asynchronous task that runs at a defined interval.

    Attributes:
        event_loop (asyncio.AbstractEventLoop): The asyncio event loop managing the task.
        interval_sec (float): The interval, in seconds, at which the task runs.
        callback (Callable): The function to execute.
        args (tuple): Positional arguments for the task.
        kwargs (dict): Keyword arguments for the task.

    Methods:
        cancel():
            Cancels the scheduled task.
    """
    def __init__(self, event_loop: asyncio.AbstractEventLoop, interval_sec: float, callback, *args, **kwargs):
        """
        Initializes the Timer instance.

        Args:
            event_loop (asyncio.AbstractEventLoop): The event loop to run the task in.
            interval_sec (float): Interval in seconds between task executions.
            callback (Callable): The task function to execute.
            *args: Positional arguments for the callback.
            **kwargs: Keyword arguments for the callback.
        """
        self._event_loop = event_loop
        self._interval_sec = interval_sec
        self._callback = callback
        self._args = args
        self._kwargs = kwargs
        self._task = self._event_loop.create_task(self._job())

    async def _job(self):
        """
        The main job loop that executes the callback at the defined interval.
        """
        while True:
            await asyncio.sleep(self._interval_sec)
            try:
                await self._callback(*self._args, **self._kwargs)
            except Exception as e:
                logger.error(f"Error in task: {e}")


    def cancel(self):
        """
        Cancels the running task.
        """
        self._task.cancel()


async def main(event_loop: asyncio.AbstractEventLoop):
    """
    Initializes and starts all tasks defined in the `all_tasks` list.

    Args:
        event_loop (asyncio.AbstractEventLoop): The asyncio event loop.

    Raises:
        ValueError: If task definitions in `all_tasks` are invalid.
    """
    
    global timers
    for task_props in all_tasks:
        args = (task_props["arg1"],) if task_props["arg1"] is not None else ()
        timers.append(Timer(event_loop, task_props["interval"], task_props["task"], *args))
    
    await asyncio.sleep(2)
    logger.info("Finshed initializing tasks...")


if __name__ == "__main__":
    """
    Entry point for the script. Sets up the event loop and starts the task scheduler.
    Handles graceful shutdown on keyboard interrupt.
    """
    try:
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)
        logger.info("Starting main process...")
        event_loop.run_until_complete(main(event_loop))
        event_loop.run_forever()
    except KeyboardInterrupt:
        logger.warning('\nCtrl-C (SIGINT) caught. Exiting...')
    finally:
        for timer in timers:
            logger.info(f"Cancelling timer: {timer.name}")
            timer.cancel()
        if event_loop.is_running():
            logger.info("Closing event loop...")
            event_loop.close()
