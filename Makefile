
build:
	go build -v -a -o docker/coin-logger

dock:
	docker build -t sudachen/coin-logger:latest docker

push:
	docker push sudachen/coin-logger:latest

restart: build dock push
	cd infra && make enc
	cd infra && make coin-logger.Restart

up:
	cd infra && make coin-logger.Up

down:
	cd infra && make coin-logger.Down

ssh:
	cd infra && make coin-logger.Ssh

checkin:
	cd infra; make enc; git commit -am "$${mesg:-updated}" || true; git push || true
	cd .deps/coin-exchange; git commit -am "$${mesg:-updated}" || true
	cd .deps/logger; git commit -am "$${mesg:-updated}" || true
	git commit -am "$${mesg:-updated}" || true
	git push --recurse-submodules=on-demand

