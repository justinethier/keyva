all: 
	cd cmd/conv; go build
	cd cmd/datagen; go build
	cd cmd/httpgen; go build
	cd cmd/keyva-cli; go build
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
	go fmt cmd/conv/*.go
	go fmt cmd/datagen/*.go
	go fmt cmd/httpgen/*.go
	go fmt cmd/keyva-cli/*.go
	go fmt cmd/keyva/*.go

bench:
	go test $(PACKAGES) -bench=.

test:
	go test $(PACKAGES)

# Integration testing

# TODO: bench.sh, test.sh
