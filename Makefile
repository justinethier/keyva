all: 
	cd cmd/keyva; go build

PACKAGES=./cache/... ./appendlog/... ./lsm/...

.phony: clean

clean:
	git clean -fdx

# Unit testing
.phony: bench test fmt doc

# Start documentation web server
doc:
	godoc -http=:6060

fmt:
	go fmt $(PACKAGES)
	go fmt *.go

bench:
	go test $(PACKAGES) -bench=.

test:
	go test $(PACKAGES)

# Integration testing

# TODO: bench.sh, test.sh
