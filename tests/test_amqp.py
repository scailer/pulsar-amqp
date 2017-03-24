import os
import unittest
import asyncio

from amqp_client.amqp import AMQPClient, ChannelContext


class TestAMQP(unittest.TestCase):
    def setUp(self):
        AMQPClient.check_consumers_period = .05
        self.amqp = AMQPClient(dsn='amqp://fake')

    async def test_basic(self):
        args, kwargs = await self.amqp.basic_publish('{}', '', 'route')
        self.assertTrue(args)
        args, kwargs = await self.amqp.basic_consume(lambda: 1, 'route')
        self.assertTrue(args)
        args, kwargs = await self.amqp.basic_get('route')
        self.assertTrue(args)
        args, kwargs = await self.amqp.basic_qos()

    async def test_exchange(self):
        args, kwargs = await self.amqp.exchange_declare('route', 'direct')
        self.assertTrue(args)
        args, kwargs = await self.amqp.exchange_delete('route')
        self.assertTrue(args)
        args, kwargs = await self.amqp.exchange_bind('route', 'source', 'key')
        self.assertTrue(args)
        args, kwargs = await self.amqp.exchange_unbind('route', 'source', 'key')
        self.assertTrue(args)

    async def test_queue(self):
        args, kwargs = await self.amqp.queue_declare('route')
        self.assertTrue(args)
        args, kwargs = await self.amqp.queue_delete('route')
        self.assertTrue(args)
        args, kwargs = await self.amqp.queue_bind('route', 'ex', 'key')
        self.assertTrue(args)
        args, kwargs = await self.amqp.queue_unbind('route', 'ex', 'key')
        self.assertTrue(args)
        args, kwargs = await self.amqp.queue_purge('route')
        self.assertTrue(args)

    async def test_disconnect(self):
        await self.amqp.basic_consume(lambda: 1, 'route')
        connection = await self.amqp.pool.connect()
        connection.connection_lost(exc=Exception())
        self.assertTrue(self.amqp._disconnected_consumers)
        await asyncio.sleep(.1)
        self.assertFalse(self.amqp._disconnected_consumers)

    async def passtest_reuse_channel(self):
        connection = await self.amqp.pool.connect()
        print(connection.free_channel_ids)
        async with ChannelContext(connection) as channel:
            channel.channel_id
        print(connection.free_channel_ids)
        async with ChannelContext(connection) as channel:
            channel.channel_id
        print(connection.free_channel_ids)
