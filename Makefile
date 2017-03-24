all: clean test

clean:
	rm -rf build && rm -rf htmlcov && rm -rf .coverage && rm -rf .cache && rm -rf dist && rm -rf pulsar_amqp.egg-info

test: SHELL:=/bin/bash
test: 
	python3 runtests.py --coverage && coverage html --include='amqp_client/*'

pypi:
	python2 setup.py sdist upload
