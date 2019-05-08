build:
	@(cd cmd/server; GO111MODULE=on go build -o server .)
	@(cd cmd/client; GO111MODULE=on go build -o client .)

clean:
	@rm -f cmd/server/server cmd/client/client

init:
	@GO111MODULE=on go mod init

test:
	@t=`mktemp -t cover`; \
	go test -coverprofile=$$t ./... && go tool cover -func=$$t && unlink $$t
