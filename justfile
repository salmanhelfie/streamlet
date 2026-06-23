# Streamlet developer tasks. Run `just` to list, `just check` before pushing.

# Show available recipes.
default:
    @just --list

# Everything CI runs: format check, clippy, and the full test suite.
check: fmt-check clippy test

# Format the whole workspace in place.
fmt:
    cargo fmt --all

# Verify formatting without changing files.
fmt-check:
    cargo fmt --all -- --check

# Lint with clippy, treating warnings as errors.
clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run the full test suite across all features.
test:
    cargo test --workspace --all-features

# Run the in-process counter demo.
counter:
    cargo run -p counter-example --bin counter --features libsql

# Run the typed bank-account demo.
bank:
    cargo run -p bank-account-example --bin bank-account --features libsql
