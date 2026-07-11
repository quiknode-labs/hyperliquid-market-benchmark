# Contributing

Contributions that improve measurement correctness, reproducibility, or the
clarity of disclosed limitations are welcome.

Before opening a pull request:

```bash
cargo fmt --all --check
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
```

Changes to the receipt boundary, canonicalization, cohort admission, clock
gate, quantile algorithm, publication cadence, or event schema are measurement
changes. They must:

1. update `docs/METHODOLOGY.md` and `docs/DATA_DICTIONARY.md`;
2. increment the measurement version;
3. add deterministic boundary and failure-path tests; and
4. explain whether old and new observations are comparable.

Never commit a live API token, provider URL containing a customer identifier,
SSH inventory, IP address list, or internal runner hostname. Examples must use
reserved names and deliberately invalid credentials.
