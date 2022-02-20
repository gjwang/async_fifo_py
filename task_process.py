import asyncio
import logging

from bin.transfer_process.async_fifo import TaskFiFo
from common_base.singleton import Singleton


class TxProcessor(metaclass=Singleton):
    def __init__(self):
        self.task_q = TaskFiFo()
        asyncio.ensure_future(self.do_task_forever())

    async def put(self, task):
        logging.info(f"task={task} args={task.args} process={task.process}")
        fut = await self.task_q.put(task)
        return await fut

    async def do_task_forever(self):
        logging.info("TxProcessor do_task_forever...")
        while True:
            try:
                task = await self.task_q.get()
                fut, tsk = task
                result = await self.dispatch(tsk, fut)
                fut.set_result(result)
            except Exception as ex:
                logging.exception(f"do_task_forever error, ex={ex}")

    async def dispatch(self, tsk, fut):
        try:
            return await tsk.process(**tsk.args)
        except Exception as ex:
            fut.set_exception(ex)
            logging.exception(f"TxProcessor dispatch error, ex={ex}")


txProcessorIns = TxProcessor()
