build:
	@(cd cmd/server; GO111MODULE=on go build -o server .)

init:
	@GO111MODULE=on go mod init

test:
	@t=`mktemp -t cover`; \
	go test -coverprofile=$$t ./... && go tool cover -func=$$t && unlink $$t
