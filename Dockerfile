# Use an older, highly compatible Debian GNU Linux base
FROM rust:bullseye

# Install the standard C libraries, clang, and libpcap for XDP
RUN apt-get update && apt-get install -y \
    libelf-dev \
    zlib1g-dev \
    libpcap-dev \
    pkg-config \
    clang \
    llvm

# Install rustfmt (required to fix the xbricks/ipblower skeleton panic)
RUN rustup component add rustfmt

# Set the working directory
WORKDIR /app