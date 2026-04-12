FROM rust:1.94-slim-bullseye

# 1. Use the ARCHIVE mirrors because Bullseye backports have been moved
RUN echo "deb http://archive.debian.org/debian bullseye-backports main" | tee /etc/apt/sources.list.d/backports.list

# 2. Tell APT to ignore the fact that the archive hasn't been updated in a while (expired keys/timestamps)
RUN echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until

# 3. Update and Install
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
    libpcap-dev  # <-- ADD THIS

# 4. Install libxdp from the archive-backports
RUN apt-get install -y -t bullseye-backports libxdp-dev
RUN rustup component add rustfmt
# Set the default working directory for all future commands
WORKDIR /app
