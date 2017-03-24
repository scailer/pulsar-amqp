CHANNEL_METHODS = ('basic_publish', 'basic_get', 'basic_qos', 'basic_consume',
                   'exchange_declare', 'exchange_delete', 'exchange_unbind',
                   'exchange_bind', 'queue_purge', 'queue_unbind', 'queue_bind',
                   'queue_delete', 'queue_declare')


class FakeTransport(object):
    def get_extra_info(self, *args):
        return '127.0.0.1:5672'

    def set_write_buffer_limits(self, *args, **kwargs):
        pass


class FakeChannel(object):
    channel_id = 1

    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        if name in CHANNEL_METHODS:
            return self.execute
        return super().__getattr__(name)

    async def execute(self, *args, **kwargs):
        return args, kwargs
