import aio_pika
import asyncio

from typing import Any
from aio_pika import IncomingMessage, Queue


AIO_PIKA_VERSION_TO_PATCH = "9.4.1"


def apply_patch():
    if aio_pika.__version__ != AIO_PIKA_VERSION_TO_PATCH:
        return


    def __init__(self, queue: Queue, **kwargs: Any):
        self._consumer_tag: ConsumerTag
        self._amqp_queue: AbstractQueue = queue
        self._queue = asyncio.Queue()
        self._consume_kwargs = kwargs

        if kwargs.get("timeout"):
            self.__anext_impl = self.__anext_with_timeout
        else:
            self.__anext_impl = self.__anext_without_timeout

        self._amqp_queue.close_callbacks.add(self.close)


    async def __anext__(self) -> IncomingMessage:
        return await self.__anext_impl()


    async def __anext_with_timeout(self) -> IncomingMessage:
        if not hasattr(self, "_consumer_tag"):
            await self.consume()
        try:
            return await asyncio.wait_for(
                self._queue.get(),
                timeout=self._consume_kwargs.get("timeout"),
            )
        except asyncio.CancelledError:
            timeout = self._consume_kwargs.get(
                "timeout",
                self.DEFAULT_CLOSE_TIMEOUT,
            )
            log.info(
                "%r closing with timeout %d seconds",
                self, timeout,
            )
            await asyncio.wait_for(self.close(), timeout=timeout)
            raise


    async def __anext_without_timeout(self) -> IncomingMessage:
        if not hasattr(self, "_consumer_tag"):
            await self.consume()
        try:
            return await self._queue.get()
        except asyncio.CancelledError:
            await self.close()
            raise


    aio_pika.queue.QueueIterator.__anext_with_timeout = __anext_with_timeout
    aio_pika.queue.QueueIterator.__anext_without_timeout = \
        __anext_without_timeout
    aio_pika.queue.QueueIterator.__anext__ = __anext__
    aio_pika.queue.QueueIterator.__init__ = __init__


apply_patch()
