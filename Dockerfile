FROM rust:1.94-slim-bookworm

# 1. Update and Install ALL dependencies from the standard Bookworm repos
# We include both libpcap-dev and libxdp-dev right here.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    clang \
    llvm \
    libelf-dev \
    pkg-config \
    build-essential \
    git \
    ca-certificates \
    m4 \
    libpcap-dev \
    libxdp-dev && \
    rm -rf /var/lib/apt/lists/*

# 2. Install rustfmt
RUN rustup component add rustfmt

# 3. Set the default working directory
WORKDIR /app