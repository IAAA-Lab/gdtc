build:
	docker build -t gdtc/base:latest .
	docker-compose -f deployment/docker-compose.yml up -d

test:
	docker exec gdtc /bin/bash -c "source activate gdtc && cd gdtc && python -m unittest -v gdtc/tests/filter_tests.py"