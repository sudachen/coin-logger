
build:
	go build -v -a -o docker/coin-logger

dock:
	docker build -t sudachen/coin-logger:latest docker

push: build dock
	docker push sudachen/coin-logger:latest

restart: push
	cd infra && make coin-logger.Restart
	
	

