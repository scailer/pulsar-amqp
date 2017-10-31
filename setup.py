# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name='pulsar-amqp',
    version='0.2.0',
    author='Dmitriy Vlasov',
    author_email='scailer@yandex.ru',

    packages=['amqp_client'],
    include_package_data=True,
    #install_requires=['aioamqp==0.10.0'],
    requires=['aioamqp', 'pulsar (>= 1.5.4)'],

    url='https://github.com/scailer/pulsar-aioamqp',
    license='MIT license',
    description='Pulsar wrapper for aioamqp',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
