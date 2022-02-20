import asyncio
import logging

from common_base.async_fifo.async_fifo import TaskFiFo
from common_base.singleton import Singleton


class TaskProcessorBase(metaclass=Singleton):
    '''
    User can inherit TaskProcessorBase, and impl dispatch
    '''
    def __init__(self):
        self.task_q = TaskFiFo()
        asyncio.ensure_future(self.do_task_forever())

    async def put(self, task):
        logging.info(f"put task={task}")
        fut = await self.task_q.put(task)
        return await fut

    async def do_task_forever(self):
        logging.info("TxProcessor do_task_forever...")
        while True:
            try:
                fut, task = await self.task_q.get()
                result = await self.dispatch(task)
                fut.set_result(result)
            except Exception as ex:
                fut.set_exception(ex)
                logging.exception(f"do_task_forever error, ex={ex}")

    async def dispatch(self, task):
        '''
        demo process msg, use should impl
        '''
        try:
            print(f"echo task={task}")
            return task
        except Exception as ex:
            logging.exception(f"TxProcessor dispatch error, ex={ex}")
            raise ex  # raise ex so fut can deliver it to caller
