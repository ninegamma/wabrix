.PHONY: build clean

# The default action when you just type 'make'
build:
	docker run --rm -v $(shell pwd):/app ebpfl-builder cargo build --release
	# Fix ownership so you can move/edit the binary on the host
	sudo chown -R $(shell id -u):$(shell id -g) target/

clean:
	cargo clean
