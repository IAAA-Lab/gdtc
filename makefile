context:
	mkdir -p /input
	mkdir -p /output
	git clone https://github.com/IAAA-Lab/gdtc-test-data.git
	cp -rf ./gdtc-test-data/input_files/* /input
build:
	docker build -t gdtc/base:latest .
	docker-compose -f deployment/docker-compose.yml up -d

test:
	docker exec gdtc /bin/bash -c "source activate gdtc && cd gdtc && python -m unittest -v gdtc/tests/filter_tests.py"