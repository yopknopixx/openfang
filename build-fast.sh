#!/bin/bash
# Fast build for development - uses thin LTO + 8 codegen units
# ~3-4x faster than full release builds
cd /home/yog/openfang-build
touch crates/openfang-hands/src/bundled.rs  # force recompile of bundled HAND.toml files
cargo build --profile dev-release --bin openfang "$@"
