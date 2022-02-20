import asyncio


class TaskFiFo(object):
    def __init__(self):
        self._fifo = asyncio.Queue()

    async def put(self, msg):
        fut = asyncio.Future()
        tsk = (fut, msg)
        await self._fifo.put(tsk)
        return fut

    async def get(self):
        return await self._fifo.get()

    def qsize(self):
        return self._fifo.qsize()
