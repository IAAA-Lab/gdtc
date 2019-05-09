context:
	mkdir -p ${LOCAL_IN_VOL}
	mkdir -p ${LOCAL_OUT_VOL}
	git clone https://github.com/IAAA-Lab/gdtc-test-data.git
	cp -rf ./gdtc-test-data/input_files/* ${LOCAL_IN_VOL}
	rm -rf gdtc-test-data


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

# If you have already run make context in your computer, this will be faster than "all"
local: build run test

all: context local
