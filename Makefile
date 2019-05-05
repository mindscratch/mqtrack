build:
	@(cd cmd/server; GO111MODULE=on go build -o server .)

init:
	@GO111MODULE=on go mod init

