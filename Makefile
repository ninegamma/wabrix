.PHONY: build clean

NATIVE_MEMBERS = -p ipblower 

# The default action when you just type 'make'
build:
build:
	# 1. Mount the whole 'rust' folder to /app
	# 2. Set the working directory to /app/wabrix
	docker run --rm \
		-v /home/rainer1956/rust:/app \
		-w /app/xbrix \
		ebpfl-builder cargo build --release --no-default-features $(NATIVE_MEMBERS)

	sudo chown -R $(shell id -u):$(shell id -g) target/

clean:
	cargo clean
