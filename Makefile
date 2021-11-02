.phony: clean

clean:
	git clean -fdx

# Unit testing
.phony: bench test

bench:
	cd cache ; go test -bench=.

test:
	cd cache ; go test
	cd lsb; go test


# Integration testing

# TODO: bench.sh, test.sh
