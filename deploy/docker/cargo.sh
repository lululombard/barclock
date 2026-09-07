#!/bin/sh
# Run any cargo command inside the arm64 Debian trixie build container, with the crate registry
# and the target directory kept in named Docker volumes so rebuilds are incremental.
#   deploy/docker/cargo.sh build --release
#   deploy/docker/cargo.sh test
#   deploy/docker/cargo.sh run --release -- --preview /src/preview.png
# The image must exist: docker build --platform linux/arm64 -t barclock-build deploy/docker
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
exec docker run --rm --platform linux/arm64 \
    -v "$ROOT":/src -v barclock-cargo:/usr/local/cargo/registry -v barclock-target:/src/target \
    -w /src -e CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-8}" -e CARGO_TERM_COLOR=never \
    ${RUST_LOG:+-e RUST_LOG="$RUST_LOG"} \
    barclock-build cargo "$@"
