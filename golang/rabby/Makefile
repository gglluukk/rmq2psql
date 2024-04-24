VERSION := "1.0"
MODNAME := "rabby"
LDFLAGS := "-s -w -X main.Version=${VERSION} '-extldflags=-v -static'"

default:
	@if [ ! -f go.mod ] ; then \
		go mod init $(MODNAME) ; fi
	@if [ ! -f go.sum ] ; then \
		go mod tidy ; fi
	go fmt $(MODNAME).go
	CGO_ENABLED=0 go build -trimpath -v -ldflags $(LDFLAGS) -o $(MODNAME)
	strip $(MODNAME)

