use txreader::tx;
use futures::executor::block_on;
use txreader::tx::{Account};
use std::io;

#[test]
fn test_read() -> Result<(), anyhow::Error> {
    assert_eq!(block_on(read_multiple_files())?, ());
    Ok(())
}

async fn read_multiple_files() -> Result<(), anyhow::Error> {
    let mut l: Vec<Account> = vec![];
    for _ in 0..10000 {
        let mut vec = tx::accounts_from_path(&std::path::PathBuf::from("transactions.csv")).await?;
        l.append(&mut vec);
    }
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    tx::print_accounts_with(&mut lock, &l).await;
    Ok(())
}