# Build image for barclock: arm64 Debian trixie with exactly the dev packages the Pi has,
# so the produced binary links against the same glibc/libinput/libgbm/libudev versions.
# Usage (on Apple Silicon or any arm64 host, or with qemu on x86_64):
#   docker build --platform linux/arm64 -t barclock-build deploy/docker
#   docker run --rm --platform linux/arm64 -v "$PWD":/src -v barclock-cargo:/root/.cargo/registry \
#       -v barclock-target:/src/target -w /src barclock-build cargo build --release
FROM debian:trixie
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        git build-essential pkg-config curl ca-certificates \
        libgbm-dev libxkbcommon-dev libudev-dev libinput-dev libseat-dev \
        libgles-dev libegl-dev libdrm-dev libfontconfig-dev libfreetype-dev \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable \
    && rustc --version && cargo --version
WORKDIR /src
