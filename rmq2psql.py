#!/usr/bin/python3

import aio_pika
import argparse
import asyncio
import asyncpg
from typing import Tuple

import msgspec
import orjson
import ujson

import aio_pika_patch


class Message(msgspec.Struct):
    content: str
    message_number: int


def profile_wrapper(func):
    async def wrapper(*args, **kwargs):
        global cli
        if cli.profile:
            return await profile(func)(*args, **kwargs)
        else:
            return await func(*args, **kwargs)
    return wrapper


class RMQProcessor:
    def __init__(self, max_bulks, max_reads, json, loop_type):
        self.host = 'localhost'
        self.rmq_port = 5672
        self.sql_port = 5432
        self.user = 'test_user'
        self.password = 'testuser'
        self.vhost = self.db_name = 'test_vhost'
        self.queue_name = self.table_name = 'test_queue'
        self.max_bulks = max_bulks
        self.max_reads = max_reads
        self.json = json
        self.loop_type = loop_type
        self.pgpool = None
        self.bulk_messages = []
        self.read_counter = 0
        self.bulk_counter = 0
        self.json_processor = self.JsonProcessor()
        self.json_methods = ['ujson', 'orjson', 'msgspec', 'msgspec_struct']


    class JsonProcessor:
        @profile_wrapper
        async def ujson(self, message_decoded: str) -> Tuple[str, int]:
            msg: dict = ujson.loads(message_decoded)
            return msg['content'], msg['message_number']

        @profile_wrapper
        async def orjson(self, message_decoded: str) -> Tuple[str, int]:
            msg: dict = orjson.loads(message_decoded)
            return msg['content'], msg['message_number']

        @profile_wrapper
        async def msgspec(self, message_decoded: str) -> Tuple[str, int]:
            msg: dict = msgspec.json.decode(message_decoded)
            return msg['content'], msg['message_number']

        @profile_wrapper
        async def msgspec_struct(self, message_decoded: str) -> Tuple[str, int]:
            msg = msgspec.json.decode(message_decoded, type=Message)
            return msg.content, msg.message_number


    @profile_wrapper
    async def rmq_callback(
            self, 
            message: aio_pika.IncomingMessage = None,
            flush: bool = False
    ) -> None:
        if not flush:
            message_decoded = message.body.decode()

            if self.json == 'all':
                for method_name in self.json_methods:
                    content, message_number = \
                    await getattr(self.json_processor, 
                                  method_name)(message_decoded)
            else:  
                content, message_number = await getattr(self.json_processor, 
                                                  self.json)(message_decoded)

            self.bulk_messages.append((content, message_number))
            self.bulk_counter += 1

        if self.bulk_counter >= self.max_bulks or (
                flush and self.bulk_counter):
            async with self.pgpool.acquire() as connection:
                async with connection.transaction():
                    await connection.executemany(
                        f"INSERT INTO {self.table_name} " + 
                         "(content, message_number) VALUES ($1, $2)",
                                self.bulk_messages)
                    self.bulk_messages = []
                    self.bulk_counter = 0 

    @profile_wrapper
    async def main(self) -> None:
        self.pgpool = await asyncpg.create_pool(
                                host=self.host, port=self.sql_port,
                                user=self.user, password=self.password, 
                                database=self.db_name)
        connection = await aio_pika.connect_robust(
                                host=self.host, port=self.rmq_port,
                                login=self.user, password=self.password, 
                                virtualhost=self.vhost)

        async with connection:
            channel = await connection.channel()
            if self.loop_type == 'message_processing':
                await channel.set_qos(prefetch_count=0)
            else:
                await channel.set_qos(prefetch_count=self.max_bulks)
            queue = await channel.declare_queue(self.queue_name, durable=True)

            if self.loop_type == 'message_processing':
                async for message in queue:
                    async with message.process():
                        await self.rmq_callback(message=message)
                        self.read_counter += 1
                        if self.read_counter >= self.max_reads:
                            break
            else:
                if self.loop_type == 'queue_iteration_with_timeout':
                    queue_iterator_impl = queue.iterator(timeout=0.1)
                if self.loop_type == 'queue_iteration_without_timeout':
                    queue_iterator_impl = queue.iterator()
                try:
                    async with queue_iterator_impl as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                await self.rmq_callback(message=message) 
                                self.read_counter += 1
                                if self.read_counter >= self.max_reads:
                                    break
                except asyncio.TimeoutError:
                    print("Exit: no more messages during timeout")

        await self.rmq_callback(flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='From RabbitMQ to PostgreSQL')
    parser.add_argument('--profile', action='store_true',
                    help='enable profiling')
    parser.add_argument('--max-bulks', type=int, default=100,
                    help='max messages for bulk insert (default: 100)')
    parser.add_argument('--max-reads', type=int, default=10000,
                    help='max messages to read from queue (default: 1000)')
    parser.add_argument('--json', choices=['msgspec', 'msgspec_struct',
                                           'orjson', 'ujson', 'all'], 
                    default='ujson', help='json lib to use (default: ujson)')
    parser.add_argument('--loop-type', 
                    choices=['message_processing',
                             'queue_iteration_with_timeout', 
                             'queue_iteration_without_timeout'],
                    default='message_processing',
                    help='type of loop operation (default: message_processing)')
    cli = parser.parse_args()

    rmq_processor = RMQProcessor(cli.max_bulks, cli.max_reads, 
                                 cli.json, cli.loop_type)
    asyncio.run(rmq_processor.main())

