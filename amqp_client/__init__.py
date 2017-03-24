from pulsar.utils.config import Global
from .amqp import AMQPClient  # noqa


class AmqpServer(Global):
    name = 'amqp_server'
    flags = ['--amqp-server']
    meta = "CONNECTION_STRING"
    default = 'amqp://localhost:5672/'
    desc = 'Default connection string for the amqp server'
