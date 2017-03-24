# -*- coding: utf-8 -*-

from pulsar.apps.test import TestSuite
from amqp_client import AmqpServer  # noqa


if __name__ == '__main__':
    TestSuite(modules=['tests']).start()
