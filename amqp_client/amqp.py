import aioamqp
import asyncio
import logging

from pulsar import ProtocolConsumer, Connection
from pulsar import ensure_future, AbstractClient, Pool
from functools import partial
from .fake import FakeTransport, FakeChannel

LOG = logging.getLogger('pulsar.amqp')


class ChannelContext(object):
    '''
        Make channels poll and reuse it
    '''

    def __init__(self, connection, hold=False, reuse=False):
        self.connection = connection
        self.reuse = reuse
        self.hold = hold

    async def __aenter__(self):
        try:
            channel_id = self.connection.reuse_channel_ids.pop()
            self.channel = self.connection.channels[channel_id]
        except KeyError:
            self.channel = await self.connection.channel()

        return self.channel

    async def __aexit__(self, type, value, traceback):
        if self.hold:
            LOG.info('Hold channel %s', self.channel.channel_id)
            return True

        if self.reuse:
            self.connection.reuse_channel_ids.add(self.channel.channel_id)
        else:
            await self.channel.close()

        return True


class AMQPProtocol(ProtocolConsumer):
    ''' Proxy to channel '''

    def start_request(self):
        operation, args, kwargs = self._request
        func = getattr(self, operation, self.make_operation)
        ensure_future(func(operation, *args, **kwargs), loop=self._loop)

    async def make_operation(self, operation, *args, **kwargs):
        result = None
        hold = operation == 'basic_consume'
        reuse = operation == 'basic_publish'

        async with ChannelContext(self.connection, hold, reuse) as channel:
            result = await getattr(channel, operation)(*args, **kwargs)

        self.finished(result)


class AMQPConnection(Connection, aioamqp.AmqpProtocol):
    ''' Connection to amqp service'''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        aioamqp.AmqpProtocol.__init__(self, *args, **kwargs)
        self._consumers = []

    def connection_made(self, transport):
        aioamqp.AmqpProtocol.connection_made(self, transport)
        super().connection_made(transport)

    def connection_lost(self, *args, **kwargs):
        LOG.warn('ANQP connection lost')
        aioamqp.AmqpProtocol.connection_lost(self, *args, **kwargs)
        if self._consumers:
            LOG.warn('Backup consumers: %s', [x[0] for x in self._consumers])
            self.producer._disconnected_consumers.extend(self._consumers)
        return super().connection_lost(*args, **kwargs)

    def close(self):
        ensure_future(aioamqp.AmqpProtocol.close(self))
        super().close()

    def data_received(self, data):
        aioamqp.AmqpProtocol.data_received(self, data)

    async def execute(self, operation, *args, **kwargs):
        consumer = self.current_consumer()
        consumer.start((operation, args, kwargs))
        result = await consumer.on_finished
        return result

    async def consume(self, *args, **kwargs):
        LOG.debug('Add consumer: %s', args)
        self._consumers.append((args, kwargs))
        result = await self.execute('basic_consume', *args, **kwargs)
        return result


class FakeAMQPConnection(AMQPConnection):
    async def channel(self):
        return FakeChannel(self)


class AMQPClient(AbstractClient):
    '''
        Client for amqp broker

        > def receiver(channel, body, envelope, properties):
        >     print('[x]', channel, body, envelope, properties)
        >     await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
        >
        > cli = AMQPClient(cfg.settings.get('amqp_server').value, loop=loop)
        > await cli.queue_declare(queue_name='hello')
        > await cli.basic_consume(callback=receiver, queue_name='hello')
        > await cli.basic_publish('Hello!', '', 'hello')
    '''

    protocol_factory = partial(AMQPConnection, AMQPProtocol)
    max_reconnects = 5
    check_consumers_period = 10

    def __init__(self, dsn, pool_size=1, loop=None):
        super().__init__(loop)
        self.dsn = dsn

        if dsn in ('amqp://fake', ):
            LOG.warn('FAKE AMQP')
            self.pool = Pool(self.fake_connect, pool_size, self._loop)
            self.pool.is_connection_closed = lambda *args: False
        else:
            self.pool = Pool(self.connect, pool_size, self._loop)

        self._disconnected_consumers = []
        self._loop.call_later(
            self.check_consumers_period,
            lambda: ensure_future(self.check_consumers()))

    async def check_consumers(self):
        self._loop.call_later(
            self.check_consumers_period,
            lambda: ensure_future(self.check_consumers()))

        while self._disconnected_consumers:
            args, kwargs = self._disconnected_consumers.pop()
            LOG.info('Reconnect consumer: %s', args)

            try:
                await self.basic_consume(*args, **kwargs)
            except:
                self._disconnected_consumers.append((args, kwargs))

    async def fake_connect(self, protocol_factory=None, reconnect=0):
        connection = FakeAMQPConnection(
            partial(AMQPProtocol, loop=self._loop),
            producer=self, loop=self._loop)

        connection.connection_made(FakeTransport())
        return connection

    async def connect(self, protocol_factory=None, reconnect=0):
        LOG.debug('New AMQP connection')
        protocol_factory = protocol_factory or self.create_protocol

        try:
            fut = aioamqp.from_url(self.dsn, protocol_factory=protocol_factory)
            transport, connection = await fut

        except Exception as e:
            LOG.error(e, exc_info=True)

            if reconnect > self.max_reconnects:
                LOG.fatal('Lost AMQP connection')
                self.close()
                return

            await asyncio.sleep(5)
            connection = await self.connect(protocol_factory, reconnect + 1)

        connection.reuse_channel_ids = set()
        return connection

    async def execute(self, operation, *args, **kwargs):
        connection = await self.pool.connect()
        with connection:
            result = await connection.execute(operation, *args, **kwargs)
            return result

    # queue
    async def queue_declare(self, queue_name=None, **kwargs):
        return await self.execute('queue_declare', queue_name, **kwargs)

    async def queue_delete(self, queue_name, **kwargs):
        return await self.execute('queue_delete', queue_name, **kwargs)

    async def queue_bind(self, queue_name, exchange_name, routing_key, **kwargs):
        return await self.execute(
            'queue_bind', queue_name, exchange_name, routing_key, **kwargs)

    async def queue_unbind(self, queue_name, exchange_name, routing_key, **kwargs):
        return await self.execute(
            'queue_unbind', queue_name, exchange_name, routing_key, **kwargs)

    async def queue_purge(self, queue_name, **kwargs):
        return await self.execute('queue_purge', queue_name, **kwargs)

    # exchange
    async def exchange_declare(self, exchange_name, type_name, **kwargs):
        return await self.execute(
            'queue_declare', exchange_name, type_name, **kwargs)

    async def exchange_delete(self, exchange_name, **kwargs):
        return await self.execute('exchange_delete', exchange_name, **kwargs)

    async def exchange_bind(self, exchange_destination, exchange_source,
                            routing_key, **kwargs):
        return await self.execute(
            'exchange_bind', exchange_destination,
            exchange_source, routing_key, **kwargs)

    async def exchange_unbind(self, exchange_destination, exchange_source,
                              routing_key, **kwargs):
        return await self.execute(
            'exchange_unbind', exchange_destination,
            exchange_source, routing_key, **kwargs)

    # basic
    async def basic_publish(self, payload, exchange_name, routing_key, **kwargs):
        return await self.execute(
            'basic_publish', payload, exchange_name, routing_key, **kwargs)

    async def basic_consume(self, callback, queue_name='', **kwargs):
        connection = await self.pool.connect()
        with connection:
            result = await connection.consume(callback, queue_name, **kwargs)
            return result

    async def basic_qos(self, **kwargs):
        return await self.execute('basic_qos', **kwargs)

    async def basic_get(self, queue_name, **kwargs):
        return await self.execute('basic_get', queue_name, **kwargs)
