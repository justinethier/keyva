PACKAGES=./cache/... ./lsb/... ./sst...

.phony: clean

clean:
	git clean -fdx

# Unit testing
.phony: bench test fmt

fmt:
	go fmt $(PACKAGES)

bench:
	go test $(PACKAGES) -bench=.

test:
	go test $(PACKAGES)

# Integration testing

# TODO: bench.sh, test.sh
