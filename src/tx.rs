use anyhow::Context;
use crate::tx::TransactionKind::*;
use csv::{ReaderBuilder, Trim, WriterBuilder};
use std::sync::mpsc::{self, Receiver, Sender};
use futures::executor::ThreadPool;
use futures::future::{self, RemoteHandle};
use futures::task::SpawnExt;
use log::{debug, info};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Error, ErrorKind::{InvalidInput}};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Transaction {
    #[serde(rename = "type")]
    kind:       TransactionKind,
    #[serde(rename = "client")]
    client_id:  u16,
    #[serde(rename = "tx")]
    tx_id:      u32,
    amount:     Option<Decimal>,
}

impl Transaction {
    pub fn new( kind: TransactionKind
              , client_id: u16
              , tx_id: u32
              , a: Option<i64>
              ) -> Transaction {
        Transaction {
            kind,
            client_id,
            tx_id,
            amount: a.and_then(|x| Some(Decimal::new(x, 4)))
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all(deserialize = "lowercase", serialize = "lowercase"))]
enum TransactionKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Account {
    client_id:  u16,
    available:  Decimal,
    held:       Decimal,
    total:      Decimal,
    locked:     bool,
}

impl Account {
    fn new(client_id: u16) -> Account {
        Account {
            client_id,
            available: dec!(0.0),
            held:      dec!(0.0),
            total:     dec!(0.0),
            locked:    false
        }
    }
}

/// Reads the transactions from a file and writes the serialized results to
/// `std::io::stdout()`.
pub async fn read(path: &std::path::PathBuf) -> Result<(), anyhow::Error> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    read_with(&mut lock, path).await
}

/// Reads the transactions from a file and writes the serialized results to
/// a given `std::io::Write` writer.
pub async fn read_with(writer: &mut impl io::Write, path: &std::path::PathBuf) -> Result<(), anyhow::Error> {
    let now = std::time::Instant::now();
    let accounts = accounts_from_path(path).await?;
    info!("accounts_from_path done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    print_accounts_with(writer, &accounts).await;
    info!("print_accounts_with done. Elapsed: {:.2?}", now.elapsed());
    Ok(())
}

/// Reads the transactions from a file and returns `Vec<Account>` that
/// contains a list of parsed accounts.
pub async fn accounts_from_path(path: &std::path::PathBuf) -> Result<Vec<Account>, anyhow::Error> {
    let pool = ThreadPool::new()
        .with_context(|| format!("Could not create thread pool"))?;

    let now = std::time::Instant::now();
    let txns = deserialize(path)
        .with_context(|| format!("Could not deserialize file `{:?}`", path))?;
    info!("deserialize done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    let (all_tx, all_rx) = channels(&txns);
    info!("mpsc channels creation done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    let send = send(txns, all_tx);
    pool.spawn_ok(send);
    info!("spawn sender done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    let receive = receive(all_rx);
    let accounts = receive.await
        .with_context(|| format!("Could not receive accounts"))?;
    info!("receive.await? done. Elapsed: {:.2?}", now.elapsed());

    Ok(accounts)
}

/// Wraps the `writer` in a `csv::Writer` and writes the accounts.
/// The `csv::Writer` is already buffered so there is no need to wrap
/// `writer` in a `io::BufWriter`.
pub async fn print_accounts_with(writer: &mut impl io::Write, accounts: &Vec<Account>) {
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_writer(writer);
    accounts.iter().for_each(|account| wtr.serialize(account).unwrap());
}

/// Generate and print a list of random transactions.
pub async fn generate_txns(num_txns: u32, num_clients: u16) {
    let txns =
        (0..num_txns).fold(vec![], |mut acc, _| {
            let txn = random_txn(&acc, &num_clients);
            acc.push(txn);
            acc
        });

    let stdout = io::stdout();
    let mut lock = stdout.lock();
    print_txns_with(&mut lock, &txns).await;
}

fn random_txn(acc: &Vec<Transaction>, num_clients: &u16) -> Transaction {
    let mut rng = thread_rng();
    let (kind, client_id, tx_id, amount) =
        match acc.choose(&mut rng) {
            Some(txn) =>
                match rng.gen_range(0..=4) {
                    0 => (TransactionKind::Deposit, rng.gen_range(1..=*num_clients), rng.gen::<u32>(), Some(rng.gen::<i64>().abs())),
                    1 => (TransactionKind::Withdrawal, rng.gen_range(1..=*num_clients), rng.gen::<u32>(), Some(rng.gen::<i64>().abs())),
                    2 => (TransactionKind::Dispute, txn.client_id, txn.tx_id, None),
                    3 => (TransactionKind::Resolve, txn.client_id, txn.tx_id, None),
                    _ => (TransactionKind::Chargeback, txn.client_id, txn.tx_id, None),
                },
            None =>
                match rng.gen_range(0..=1) {
                    0 => (TransactionKind::Deposit, rng.gen_range(1..=*num_clients), rng.gen::<u32>(), Some(rng.gen::<i64>().abs())),
                    _ => (TransactionKind::Withdrawal, rng.gen_range(1..=*num_clients), rng.gen::<u32>(), Some(rng.gen::<i64>().abs())),
                }
        };
    Transaction::new(kind, client_id, tx_id, amount)
}

async fn print_txns_with(writer: &mut impl io::Write, txns: &Vec<Transaction>) {
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_writer(writer);
    txns.iter().for_each(|txn| wtr.serialize(txn).unwrap());
}

/// Reads the file from path into an ordered `Vec<Transaction>`.
fn deserialize(path: &std::path::PathBuf) -> io::Result<Vec<Transaction>> {
    let now = std::time::Instant::now();
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .trim(Trim::All)
        .from_path(path)?;
    info!("ReaderBuilder::from_path done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    let txns =
        rdr.deserialize::<Transaction>()
            .filter_map(|record| record.ok())
            .collect::<Vec<Transaction>>();
    info!("reader::deserialize done. Elapsed: {:.2?}", now.elapsed());

    Ok(txns)
}

/// Creates a `mpsc::channel` per client. Returns a `HashMap<u16, Sender<Transaction>>`
/// for all the senders and a `Vec<(u16, Receiver<Transaction>)>` for all the receivers,
/// where the `u16` is the client ID.
fn channels(txns: &Vec<Transaction>) -> (HashMap<u16, Sender<Transaction>>, Vec<(u16, Receiver<Transaction>)>) {
    txns.iter()
        .fold(
            (HashMap::new(), Vec::new()),
            | (mut map, mut vec): (HashMap<u16, Sender<Transaction>>, Vec<(u16, Receiver<Transaction>)>)
              , txn: &Transaction
            | {
                let client_id = txn.client_id;
                map.entry(client_id)
                    .or_insert_with(|| {
                        let (tx, rx) = mpsc::channel::<Transaction>();
                        vec.push((client_id, rx));
                        tx
                    });
                (map, vec)
            }
        )
}

/// Go through a `Vec<Transaction>` and send each transaction through its `Sender<Transaction>`
/// that belongs to the `client_id`
async fn send(txns: Vec<Transaction>, all_tx: HashMap<u16, Sender<Transaction>>) {
    //
    // go through all txns, look up client_id and send to tx
    //
    let now = std::time::Instant::now();
    txns.into_iter()
        .for_each(
            |txn: Transaction| {
                let client_id = txn.client_id;
                if let Some(tx) = all_tx.get(&client_id) {
                    tx.send(txn).expect("Failed to send");
                }
            });
    info!("for_each tx.send done. Elapsed: {:.2?}", now.elapsed());

    //
    // drop all tx
    //
    let now = std::time::Instant::now();
    all_tx.into_iter().for_each(|(_, tx)| drop(tx));
    info!("drop all tx done. Elapsed: {:.2?}", now.elapsed());
}

/// Use `thread_pool::ThreadPool` to spawn one task per `Receiver<Transaction>` and
/// wait for all rx to finish receiving, then returns a `Vec<Account>`.
async fn receive(all_rx: Vec<(u16, Receiver<Transaction>)>) -> io::Result<Vec<Account>> {
    let pool = ThreadPool::new()?;
    //
    // spawn handles to receive from each rx
    //
    let now = std::time::Instant::now();
    let handles =
        all_rx.into_iter()
            .map(|(client_id, rx)| {
                let handle =
                    pool.spawn_with_handle(to_account(client_id, rx))
                        .expect("Failed to spawn");
                handle
            })
            .collect::<Vec<RemoteHandle<Account>>>();
    info!("map spawn_with_handle done. Elapsed: {:.2?}", now.elapsed());

    //
    // wait for all rx to finish receiving
    //
    let now = std::time::Instant::now();
    let accounts = future::join_all(handles).await;
    info!("future::join_all(handles) done. Elapsed: {:.2?}", now.elapsed());
    Ok(accounts)
}


async fn to_account(client_id: u16, rx: Receiver<Transaction>) -> Account {
    let (account, _) =
        rx.into_iter().fold(
            (Account::new(client_id), HashMap::new()),
            | (mut account, mut handled)//: (Account, HashMap<u32, Vec<&Transaction>>)
            ,  txn//: Transaction
            | {
                let txn_id = txn.tx_id;
                match handle_txn(&mut account, &handled, &txn) {
                    // only insert when txn is ok
                    Ok(()) => handled.entry(txn_id).or_insert(vec![]).push(txn),
                    // ignore bad txns
                    _ => debug!("Ignoring invalid transaction: {:?}", txn)
                };
                (account, handled)
            });
    account
}

/// Handles a `Transaction` and updates the client's
/// `Account`. The `amount` is rounded to four digits
/// after decimal.
fn handle_txn( account: &mut Account
             , handled: &HashMap<u32, Vec<Transaction>>
             , txn:     &Transaction
             ) -> io::Result<()> {
    match txn {
        &Transaction{ kind: Deposit, amount: Some(amount), .. } => {
            (!account.locked && amount.is_sign_positive()).then(|| ())
                .ok_or(Error::from(InvalidInput))?;
            // A deposit is a credit to the client's asset account,
            // meaning it should increase the available and total
            // funds of the client account
            account.available += amount.round_dp(4);
            account.total     += amount.round_dp(4);
            Ok(())
        },
        &Transaction{ kind: Withdrawal, amount: Some(amount), .. } => {
            // If a client does not have sufficient available funds
            // the withdrawal should fail and the total amount of
            // funds should not change
            (!account.locked
                && account.available >= amount
                && amount.is_sign_positive()).then(|| ()).ok_or(Error::from(InvalidInput))?;
            // A withdraw is a debit to the client's asset account,
            // meaning it should decrease the available and total
            // funds of the client account
            account.available -= amount.round_dp(4);
            account.total     -= amount.round_dp(4);
            Ok(())
        },
        &Transaction{ kind: Dispute, tx_id, .. } => {
            // Notice that a dispute does not state the amount disputed.
            // Instead a dispute references the transaction that is
            // disputed by ID.
            let txns = handled.get(&tx_id).ok_or(Error::from(InvalidInput))?;
            // If the tx specified by the dispute doesn't exist you can
            // ignore it and assume this is an error on our partners side.
            let dispute = is_under_dispute(txns);
            let initial_txn = initial_txn(txns);
            match (dispute, initial_txn) {
                (false, Some(&Transaction{ kind: Deposit, amount: Some(amount), .. })) => {
                    // A dispute represents a client's claim that a
                    // transaction was erroneous and should be reversed.
                    // The transaction shouldn't be reversed yet but
                    // the associated funds should be held. This means
                    // that the clients available funds should decrease
                    // by the amount disputed, their held funds should
                    // increase by the amount disputed, while their
                    // total funds should remain the same.
                    account.available -= amount.round_dp(4);
                    account.held      += amount.round_dp(4);
                    Ok(())
                },
                (false, Some(&Transaction{ kind: Withdrawal, amount: Some(amount), .. })) => {
                    // NOTE: Assumes a dispute on a withdrawal temporarily
                    // puts funds into the client's held funds.
                    account.held      += amount.round_dp(4);
                    account.total     += amount.round_dp(4);
                    Ok(())
                },
                _ => Err(Error::from(InvalidInput))
            }
        },
        &Transaction{ kind: Resolve, tx_id, .. } => {
            // Like disputes, resolves do not specify an amount. Instead
            // they refer to a transaction that was under dispute by ID.
            let txns = handled.get(&tx_id).ok_or(Error::from(InvalidInput))?;
            // If the tx specified doesn't exist, or the tx isn't under
            // dispute, you can ignore the resolve and assume this is an
            // error on our partner's side.
            let dispute = is_under_dispute(txns);
            let initial_txn = initial_txn(txns);
            match (dispute, initial_txn) {
                (true, Some(&Transaction{ kind: Deposit, amount: Some(amount), .. })) => {
                    // A resolve represents a resolution to a dispute,
                    // releasing the associated held funds. Funds that
                    // were previously disputed are no longer disputed.
                    // This means that the clients held funds should
                    // decrease by the amount no longer disputed, their
                    // available funds should increase by the amount no
                    // longer disputed, and their total funds should
                    // remain the same.
                    account.available += amount.round_dp(4);
                    account.held      -= amount.round_dp(4);
                    Ok(())
                },
                (true, Some(&Transaction{ kind: Withdrawal, amount: Some(amount), .. })) => {
                    // NOTE: Assumes a resolve removes the temporarily
                    // increased funds from the client's held funds.
                    account.held      -= amount.round_dp(4);
                    account.total     -= amount.round_dp(4);
                    Ok(())
                },
                _ => Err(Error::from(InvalidInput))
            }
        },
        &Transaction{ kind: Chargeback, tx_id, .. } => {
            // Like a dispute and a resolve a chargeback refers to the
            // transaction by ID (tx) and does not specify an amount.
            let txns = handled.get(&tx_id).ok_or(Error::from(InvalidInput))?;
            // Like a resolve, if the tx specified doesn't exist, or
            // the tx isn't under dispute, you can ignore chargeback
            // and assume this is an error on our partner's side.
            let dispute = is_under_dispute(txns);
            let initial_txn = initial_txn(txns);
            match (dispute, initial_txn) {
                (true, Some(&Transaction{ kind: Deposit, amount: Some(amount), .. })) => {
                    // A chargeback is the final state of a dispute and
                    // represents the client reversing a transaction.
                    // Funds that were held have now been withdrawn.
                    // This means that the clients held funds and total
                    // funds should decrease by the amount previously
                    // disputed. If a chargeback occurs the client's
                    // account should be immediately frozen.
                    account.held   -= amount.round_dp(4);
                    account.total  -= amount.round_dp(4);
                    account.locked  = true;
                    Ok(())
                },
                (true, Some(&Transaction{ kind: Withdrawal, amount: Some(amount), .. })) => {
                    // NOTE: Assumes a chargeback to a withdrawal reverses
                    // a withdrawal, and puts the temporarily held funds
                    // back to the client available funds.
                    account.available += amount.round_dp(4);
                    account.held      -= amount.round_dp(4);
                    account.locked     = true;
                    Ok(())
                },
                _ => Err(Error::from(InvalidInput))
            }
        },
        _ => Err(Error::from(InvalidInput))
    }
}

/// Returns `true` if there are more disputes than resolves,
/// and if there has been no chargebacks.
fn is_under_dispute(txns: &Vec<Transaction>) -> bool {
    let n_dispute = txns.iter().filter(|t| t.kind == Dispute).count();
    let n_resolve = txns.iter().filter(|t| t.kind == Resolve).count();
    let chargeback = txns.iter().any(|t| t.kind == Chargeback);
    let dispute = n_dispute > n_resolve;
    dispute && !chargeback
}

/// Returns the first occurrence of a deposit or a
/// withdrawal as `Some(&Transaction)` if found.
fn initial_txn(txns: &Vec<Transaction>) -> Option<&Transaction> {
    txns.iter().filter(|t| t.kind == Withdrawal || t.kind == Deposit).next()
}

#[cfg(test)]
mod test {
    use common_macros::hash_map;
    use crate::tx::*;
    use tempfile::NamedTempFile;
    use std::io::Write;
    use futures::executor::block_on;

    #[test]
    fn test_read_with() -> Result<(), anyhow::Error> {
        let path = &std::path::PathBuf::from("transactions_simple.csv");
        let mut result = Vec::new();
        block_on(read_with(&mut result, path))?;
        let mut lines = std::str::from_utf8(&result)?.lines();
        let expected = vec![ "client_id,available,held,total,locked"
                           , "1,1.4996,0.0,1.4996,false"
                           , "2,2,0.0,2,false"
                           , "4,0.0,0.0,0.0,false"
                           , "5,0.0,0.0,0.0,false"
                           ];
        assert_eq!(true, lines.all(|l| expected.contains(&l)));
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_deposit() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,1.0001
                        deposit,1,2,+10000
                        deposit,1,3,-500.0
                        dépôt,1,4,2.0
                        bad line
                        deposit,1,5.0,4.04
                        deposit,1,6,0.00004
                        deposit,x,1,1.0
                        deposit,1,x,1.0
                        deposit,1,1,x")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(10001.0001)
                                          , held:      dec!(0.0)
                                          , total:     dec!(10001.0001)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_withdrawal() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,+10000
                        withdrawal,1,2,1
                        withdrawal,1,3,-1
                        withdrawal
                        1,2,1.0
                        withdrawal,1,4,2.0002
                        withdrawal,1,5,3.00009
                        withdrawal,1,6.0,6.0
                        with drawal,1,7,7.0
                        with drawal,1,8,100000.0
                        withdrawal,1,9,900000.0
                        withdrawal,x,1,1.0
                        withdrawal,1,x,1.0
                        withdrawal,1,1,x")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(9993.9997)
                                          , held:      dec!(0.0)
                                          , total:     dec!(9993.9997)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_dispute() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,10000.0
                        deposit,1,2,2000.0002
                        deposit,1,3,300.00003
                        deposit,1,8,8
                        withdrawal,1,4,4
                        withdrawal,1,5,5.0005
                        withdrawal,1,6,6.0006
                        withdrawal,1,7,7.00007
                        withdrawal,1,999,70000.0
                        dispute,1,1,
                        dispute,1,2,
                        dispute,1,3,
                        dispute,1,4,
                        dispute,1,5,
                        dispute,1,6,
                        dispute,1,6,
                        dispute,1,6,
                        dispute,1,7
                        dispute,1,100,
                        dispute,1,999,
                        dispute,1,999,
                        dispute,1,1000
                        dispute,1,1000,10000.0
                        dispute,2,1,
                        dispute,x,1,1.0
                        dispute,1,x,1.0
                        dispute,1,1,x")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let mut accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        accounts.sort_by_key(|a| a.client_id);
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(-14.0012)
                                          , held:      dec!(12315.0013)
                                          , total:     dec!(12301.0001)
                                          , locked:    false
                                          }
                                 , Account{ client_id: 2
                                          , available: dec!(0)
                                          , held:      dec!(0)
                                          , total:     dec!(0)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_resolve() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,10000.0
                        deposit,1,2,2000.0002
                        deposit,1,3,300.00003
                        deposit,1,8,8
                        withdrawal,1,4,4
                        withdrawal,1,5,5.0005
                        withdrawal,1,6,6.0006
                        withdrawal,1,7,7.00007
                        withdrawal,1,999,70000.0
                        dispute,1,1,
                        dispute,1,1,
                        dispute,1,1,
                        resolve,1,1,
                        resolve,1,1,
                        dispute,1,1,
                        dispute,1,2,
                        dispute,1,3,
                        dispute,1,4,
                        dispute,1,5,
                        dispute,1,6,
                        dispute,1,6,
                        dispute,1,6,
                        resolve,1,1,
                        resolve,1,2,
                        resolve,1,3,
                        resolve,1,4,
                        resolve,1,5,
                        resolve,1,6,
                        resolve,1,7,
                        resolve,1,100,
                        resolve,1,999,
                        resolve,1,999,
                        resolve,1,1000
                        resolve,1,1000,10000.0
                        resolve,2,1,
                        resolve,x,1,1.0
                        resolve,1,x,1.0
                        resolve,1,1,x")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let mut accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        accounts.sort_by_key(|a| a.client_id);
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(12285.9990)
                                          , held:      dec!(0)
                                          , total:     dec!(12285.999)
                                          , locked:    false
                                          }
                                 , Account{ client_id: 2
                                          , available: dec!(0)
                                          , held:      dec!(0)
                                          , total:     dec!(0)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_chargeback() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,10000.0
                        deposit,1,2,2000.0002
                        deposit,1,3,300.00003
                        deposit,1,8,8
                        withdrawal,1,4,4
                        withdrawal,1,5,5.0005
                        withdrawal,1,6,6.0006
                        withdrawal,1,7,7.00007
                        withdrawal,1,998,70000.0
                        deposit,1,999,998.998
                        dispute,1,1,
                        resolve,1,1,
                        dispute,1,1,
                        dispute,1,2,
                        dispute,1,3,
                        dispute,1,4,
                        dispute,1,5,
                        dispute,1,6,
                        chargeback,1,1,
                        chargeback,1,1,
                        chargeback,1,2,
                        chargeback,1,3,
                        chargeback,1,4,
                        chargeback,1,5,
                        chargeback,1,6,
                        chargeback,1,6,
                        chargeback,1,6,
                        chargeback,1,6,
                        chargeback,1,998,
                        chargeback,1,999,
                        chargeback,1,1000,
                        chargeback,1,1000,10000.0
                        chargeback,2,1,
                        chargeback,x,1,1.0
                        chargeback,1,x,1.0
                        chargeback,1,1,x
                        ")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let mut accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        accounts.sort_by_key(|a| a.client_id);
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(999.9979)
                                          , held:      dec!(0)
                                          , total:     dec!(999.9979)
                                          , locked:    true
                                          }
                                 , Account{ client_id: 2
                                          , available: dec!(0)
                                          , held:      dec!(0)
                                          , total:     dec!(0)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_withdraw_too_much() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        withdrawal,1,1,50
                        deposit,1,2,300
                        dispute,1,2,
                        deposit,1,3,100
                        withdrawal,1,4,200")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(100)
                                          , held:      dec!(300)
                                          , total:     dec!(400)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_dispute_deposit() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        dispute,1,1,
                        resolve,1,1,
                        dispute,1,1,
                        dispute,1,1,
                        dispute,1,2,")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(0)
                                          , held:      dec!(100)
                                          , total:     dec!(100)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_dispute_withdrawal() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        withdrawal,1,2,50
                        dispute,1,2,
                        resolve,1,2,
                        dispute,1,2,
                        dispute,1,2,
                        dispute,1,2,")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(50)
                                          , held:      dec!(50)
                                          , total:     dec!(100)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_resolve_many_times() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        dispute,1,1,
                        resolve,1,1,
                        resolve,1,1,
                        resolve,1,2,")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(100)
                                          , held:      dec!(0)
                                          , total:     dec!(100)
                                          , locked:    false
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_chargeback_deposit() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        dispute,1,1,
                        chargeback,1,1,
                        chargeback,1,1,
                        chargeback,1,2,
                        resolve,1,1,")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(0)
                                          , held:      dec!(0)
                                          , total:     dec!(0)
                                          , locked:    true
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_chargeback_withdrawal() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        withdrawal,1,2,50
                        dispute,1,2,
                        chargeback,1,2,
                        chargeback,1,2,
                        chargeback,1,3,
                        resolve,1,2,")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(100)
                                          , held:      dec!(0)
                                          , total:     dec!(100)
                                          , locked:    true
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_withdraw_from_locked_account() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        dispute,1,1,
                        chargeback,1,1,
                        withdrawal,1,2,50")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(0)
                                          , held:      dec!(0)
                                          , total:     dec!(0)
                                          , locked:    true
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_deposit_to_locked_account() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        dispute,1,1,
                        chargeback,1,1,
                        deposit,1,2,50")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(0)
                                          , held:      dec!(0)
                                          , total:     dec!(0)
                                          , locked:    true
                                          }
                                 ]);
        Ok(())
    }

    #[test]
    fn test_accounts_from_path_resolve_locked_account() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,100
                        dispute,1,1,
                        chargeback,1,1,
                        resolve,1,1,")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let accounts = block_on(accounts_from_path(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(0)
                                          , held:      dec!(0)
                                          , total:     dec!(0)
                                          , locked:    true
                                          }
                                 ]);
        Ok(())
    }
}