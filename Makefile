# Makefile
.PHONY: setup build deploy stop clean test

setup:
	@.scripts/init_cluster.sh

build:
	@cargo build --release --bin master
	@cargo build --release --bin worker

deploy: build
	@./dev/scripts/deploy.sh $(WORKERS)

stop:
	@docker-compose -f docker-compose.dynamic.yml down

clean: stop
	@docker-compose -f docker-compose.dynamic.yml down -v
	@rm -f docker-compose.dynamic.yml

test:
	@./dev/scripts/test-orchestration.sh

logs:
	@docker-compose -f docker-compose.dynamic.yml logs -f

ssh-master:
	@docker exec -it mapreduce-master /bin/sh

# Deploy with custom number of workers
deploy-5:
	@make deploy WORKERS=5

deploy-10:
	@make deploy WORKERS=10
