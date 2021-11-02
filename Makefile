.phony: clean

clean:
	git clean -fdx

# Unit testing
.phony: bench test

bench:
	cd cache ; go test -bench=.

test:
	cd cache ; go test -bench=.


# Integration testing

# TODO: bench.sh, test.sh
