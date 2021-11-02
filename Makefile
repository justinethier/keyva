.phony: clean

clean:
	git clean -fdx

# Unit testing
.phony: bench test

bench:
	go test ./cache/... ./lsb/... -bench=.

test:
	go test ./cache/... ./lsb/...

# Integration testing

# TODO: bench.sh, test.sh
