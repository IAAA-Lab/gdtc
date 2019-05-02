context:
	export LOCAL_IN_VOL=${HOME}/input_files
	export LOCAL_OUT_VOL=${HOME}/output_files
	export POSTGIS_HOST=postgis
	export POSTGIS_USER=postgres
	export POSTGIS_PASS=geodatatoolchainps
	export POSTGIS_DATABASE=postgres
	export POSTGIS_EXTERNAL_PORT=8432
	export POSTGIS_INTERNAL_PORT=5432
	export GDTC_IN_VOL=/input
	export GDTC_OUT_VOL=/output
	mkdir -p ${HOME}/input_files
	mkdir -p ${HOME}/output_files
	git clone https://github.com/IAAA-Lab/gdtc-test-data.git
	cp -rf ./gdtc-test-data/input_files/* ${HOME}/input_files

build:
	docker build -t gdtc/base:latest .
	docker-compose -f deployment/docker-compose.yml up -d

test:
	docker exec gdtc /bin/bash -c "source activate gdtc && cd gdtc && python -m unittest -v gdtc/tests/filter_tests.py"