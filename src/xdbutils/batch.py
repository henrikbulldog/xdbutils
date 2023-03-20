""" Class Batch for async runs """

from __future__ import annotations
import asyncio
from typing import Coroutine, Callable
from functools import wraps, partial
import nest_asyncio

def async_wrap(func):
    """ Converts callable to async """
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


class Batch(list[Callable]):
    """ Runs tasks in batches asynchronously """


    def append(self, task: Callable, *args, **kwargs) -> Batch:
        """ Append task """
        atask = async_wrap(task)
        super().append(atask(*args, **kwargs))
        return self


    def append_async(self, task: Coroutine) -> Batch:
        """ Append async task """
        super().append(task)
        return self


    async def __execute_in_parrallel(self) -> Coroutine:
        tasks = []
        for coroutine in self:
            tasks.append(coroutine)
        return await asyncio.gather(*tasks, return_exceptions=True)        


    def __call__(self):
        nest_asyncio.apply()
        asyncio.set_event_loop(asyncio.new_event_loop())
        return asyncio.get_event_loop().run_until_complete(self.__execute_in_parrallel())


    def __repr__(self):
        return f"{self.__class__.__name__}({list.__repr__(self)})" 
