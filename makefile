context:
	mkdir -p ${HOME}/input_files
	mkdir -p ${HOME}/output_files
	git clone https://github.com/IAAA-Lab/gdtc-test-data.git
	cp -rf ./gdtc-test-data/input_files/* ${HOME}/input_files

build:
	docker build -t gdtc/base:latest .
	docker-compose -f deployment/docker-compose.yml up -d
	docker ps

test:
	docker exec gdtc /bin/bash -c "source activate gdtc && cd gdtc && python -m unittest -v gdtc/tests/filter_tests.py"