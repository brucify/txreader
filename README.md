txreader
=====

A Rust CLI application

```shell
txreader 0.1.0

USAGE:
    txreader [FLAGS] [OPTIONS] <path>

FLAGS:
    -G, --generate    Generates a list of random transactions
    -h, --help        Prints help information
    -V, --version     Prints version information

OPTIONS:
    -c, --clients <num-clients>      Number of clients in the generated transactions [default: 100]
    -t, --transactions <num-txns>    Number of transactions to generate [default: 10000]

ARGS:
    <path>    Path to the csv file that contains transactions. Optional if --generate is set
```


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

Optionally, set the environment variable`RUST_LOG` to `info` or `debug` to see more logs.

    $ RUST_LOG=debug cargo run -- transactions.csv

Generate test transactions
-----

To generate a list of random transactions for testing, use the `--generate` flag:

    $ cargo run -- --generate > transactions.csv

Generate 1,000,000 (default 10,000) transactions for 500 (default 100) clients:  

    $ cargo run -- --generate -t 1000000 -c 500 > 1m_transactions.csv

Usage
-----

```rust
let fut = tx::read(&std::path::PathBuf::from("transactions.csv"));
block_on(fut).unwrap();
```
This will write the results to `std::io::stdout()` by default.

Or run multiple file using `futures::future::join_all`:

```rust
let mut futures= vec![];
let path1 = &std::path::PathBuf::from("transactions1.csv");
let path2 = &std::path::PathBuf::from("transactions2.csv");
futures.push(tx::accounts_from_path(path1));
futures.push(tx::accounts_from_path(path2));

let accounts = future::join_all(futures).await
    .into_iter()
    .filter_map(|x| x.ok())
    .fold(vec![], |mut acc, mut vec| {
        acc.append(&mut vec);
        acc
    });

let stdout = io::stdout();
let mut lock = stdout.lock();
tx::print_accounts_with(&mut lock, &accounts).await;
```

<img src="https://user-images.githubusercontent.com/1086619/128244658-08518d8c-bf59-403b-ac70-f874d884e8b4.jpg" width="700"/>

Unit tests
-----

    $ cargo test
    Finished test [unoptimized + debuginfo] target(s) in 0.06s
     Running unittests (target/debug/deps/txreader-b64430a6c1410750)

    running 18 tests
    test tx::test::test_accounts_from_path_dispute_deposit ... ok
    test tx::test::test_accounts_from_path_deposit ... ok
    test tx::test::test_accounts_from_path_dispute_withdrawal ... ok
    test tx::test::test_accounts_from_path_chargeback_withdrawal ... ok
    test tx::test::test_accounts_from_path_chargeback_deposit ... ok
    test tx::test::test_accounts_from_path_deposit_to_locked_account ... ok
    test tx::test::test_accounts_from_path_dispute ... ok
    test tx::test::test_accounts_from_path_chargeback ... ok
    test tx::test::test_accounts_from_path_withdraw_from_locked_account ... ok
    test tx::test::test_accounts_from_path_resolve_locked_account ... ok
    test tx::test::test_accounts_from_path_withdraw_too_much ... ok
    test tx::test::test_accounts_from_path_resolve_many_times ... ok
    test tx::test::test_accounts_from_path_withdrawal ... ok
    test tx::test::test_txns_map_to_accounts ... ok
    test tx::test::test_read_txns ... ok
    test tx::test::test_read_with ... ok
    test tx::test::test_accounts_from_path_resolve ... ok
    test tx::test::test_txns_to_map ... ok
    
    test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

The unit tests are included in the `mod test` module in `tx.rs`.
