use futures::executor::block_on;
use futures::future;
use std::io::{self, Write};
use std::time::Instant;
use txreader::tx::Account;
use txreader::tx;

#[test]
#[ignore]
fn test_read_multiple_files_sequentially_1() -> Result<(), anyhow::Error> {
    let now = Instant::now();
    assert_eq!(block_on(read_multiple_files_sequentially_1())?, ());
    let elapsed = now.elapsed();
    writeln!(io::stdout(), "Elapsed: {:.2?}", elapsed)?;
    Ok(())
}

#[test]
#[ignore]
fn test_read_multiple_files_sequentially_2() -> Result<(), anyhow::Error> {
    let now = Instant::now();
    assert_eq!(block_on(read_multiple_files_sequentially_2())?, ());
    let elapsed = now.elapsed();
    writeln!(io::stdout(), "Elapsed: {:.2?}", elapsed)?;
    Ok(())
}

#[test]
#[ignore]
fn test_read_multiple_files_non_blocking() -> Result<(), anyhow::Error> {
    let now = Instant::now();
    assert_eq!(block_on(read_multiple_files_non_blocking())?, ());
    let elapsed = now.elapsed();
    writeln!(io::stdout(), "Elapsed: {:.2?}", elapsed)?;
    Ok(())
}

async fn read_multiple_files_sequentially_1() -> Result<(), anyhow::Error> {
    for _ in 0..50 {
        tx::read(&std::path::PathBuf::from("transactions.csv")).await?;
    }
    Ok(())
}

async fn read_multiple_files_sequentially_2() -> Result<(), anyhow::Error> {
    let mut l: Vec<Account> = vec![];
    for _ in 0..50 {
        let mut vec = tx::accounts_from_path(&std::path::PathBuf::from("transactions.csv")).await?;
        l.append(&mut vec);
    }
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    tx::print_accounts_with(&mut lock, &l).await;
    Ok(())
}

async fn read_multiple_files_non_blocking() -> Result<(), anyhow::Error> {
    let path = &std::path::PathBuf::from("transactions.csv");
    let mut futures= vec![];
    (0..50).for_each(|_| futures.push(tx::accounts_from_path(path)));

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

    Ok(())
}