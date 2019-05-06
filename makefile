context:
	mkdir -p ${HOME}/input_files
	mkdir -p ${HOME}/output_files
	git clone https://github.com/IAAA-Lab/gdtc-test-data.git
	cp -rf ./gdtc-test-data/input_files/* ${HOME}/input_files

build:
	docker build -t gdtc/base:latest .

run:
	docker-compose -f deployment/docker-compose.yml up -d

test:
	# This is needed because if travis script is executed right afert the docker-compose has finished,
	# and the postgres service hasn't started yet, the tests fail.
	# TODO: Re-do with waitfor script
	sleep 10
	docker exec gdtc /bin/bash -c "source activate gdtc && cd gdtc && python -m unittest -v gdtc/tests/filter_tests.py"

clean:
	docker stop gdtc
	docker stop postgis
	docker rm gdtc
	docker rm postgis

all: build run test