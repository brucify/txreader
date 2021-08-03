txreader
=====

A Rust CLI application

Build
-----

    $ cargo build

Run
-----

    $ cargo run -- transactions.csv > output.csv

A sample `transactions.csv` is included.

Set `RUST_LOG` to `info` or `debug` to see more logs. 

    $ RUST_LOG=debug cargo run -- transactions.csv
Unit tests
-----

    $ cargo test

The unit tests are included in the `mod test` module in `tx.rs`. 