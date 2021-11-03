.phony: clean

clean:
	git clean -fdx

# Unit testing
.phony: bench test

bench:
	go test ./cache/... ./lsb/... ./sst/... -bench=.

test:
	go test ./cache/... ./lsb/... ./sst/...

# Integration testing

# TODO: bench.sh, test.sh
