fmt:
    cargo fmt --all

test:
    cd {{invocation_directory()}}; cargo +nightly nextest run --all-features

testt:
    cargo +nightly nextest run --all-features --workspace
