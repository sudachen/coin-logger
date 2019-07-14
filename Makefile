
build:
	go build -v -a -o docker/coin-logger
	docker build -t sudachen/coin-logger:latest docker

push: build
	docker push sudachen/coin-logger:latest

restart: push
	cd infra && make coin-logger.Restart
	
	

