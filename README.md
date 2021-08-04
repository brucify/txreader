txreader
=====

A Rust CLI application

Build
-----

    $ cargo build

Run
-----

    $ cargo run -- transactions.csv > output.csv

Or 

    $ cargo build
    $ target/debug/txreader transactions.csv > output.csv

A sample `transactions.csv` is included.

Alternatively set the environment variable`RUST_LOG` to `info` or `debug` to see more logs.

    $ RUST_LOG=debug cargo run -- transactions.csv


Usage
-----

```rust
let fut = tx::read(&std::path::PathBuf::from("transactions.csv"))
block_on(fut).unwrap();
```
This will write the results to `std::io::stdout()` by default.

Unit tests
-----

    $ cargo test
    Finished test [unoptimized + debuginfo] target(s) in 0.06s
     Running unittests (target/debug/deps/txreader-b64430a6c1410750)

    running 18 tests
    test tx::test::test_dispute_withdrawal ... ok
    test tx::test::test_deposit_to_locked_account ... ok
    test tx::test::test_deposit ... ok
    test tx::test::test_chargeback_withdrawal ... ok
    test tx::test::test_dispute_deposit ... ok
    test tx::test::test_chargeback_deposit ... ok
    test tx::test::test_txns_map_to_accounts ... ok
    test tx::test::test_read_with ... ok
    test tx::test::test_read_txns ... ok
    test tx::test::test_dispute ... ok
    test tx::test::test_resolve_locked_account ... ok
    test tx::test::test_chargeback ... ok
    test tx::test::test_txns_to_map ... ok
    test tx::test::test_resolve_many_times ... ok
    test tx::test::test_resolve ... ok
    test tx::test::test_withdraw_from_locked_account ... ok
    test tx::test::test_withdrawal ... ok
    test tx::test::test_withdraw_too_much ... ok
    
    test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

The unit tests are included in the `mod test` module in `tx.rs`.
