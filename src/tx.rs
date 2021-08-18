use anyhow::Context;
use crate::tx::TransactionKind::*;
use csv::{ReaderBuilder, Trim, WriterBuilder};
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::executor::ThreadPool;
use futures::{StreamExt, future};
use log::{debug, info};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Error, ErrorKind::{InvalidInput}, Write};

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
pub async fn accounts_from_path0(path: &std::path::PathBuf) -> Result<Vec<Account>, anyhow::Error> {
    let now = std::time::Instant::now();
    let txns = read_txns(path).await
        .with_context(|| format!("Could not read transactions from file `{:?}`", path))?;
    info!("read_txns done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    let txns_map = txns_to_map(txns);
    info!("txns_to_map done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    let accounts = txns_map_to_accounts(txns_map).await;
    info!("txns_map_to_accounts done. Elapsed: {:.2?}", now.elapsed());

    Ok(accounts)
}

/// Reads the transactions from a file and returns `Vec<Account>` that
/// contains a list of parsed accounts.
pub async fn accounts_from_path(path: &std::path::PathBuf) -> Result<Vec<Account>, anyhow::Error> {
    let now = std::time::Instant::now();
    let pool = ThreadPool::new()?;
    let txns = read_txns(path).await
        .with_context(|| format!("Could not read transactions from file `{:?}`", path))?;
    writeln!(io::stdout(), "read_txns done. Elapsed: {:.2?}", now.elapsed())?;

    let txns_map = txns_to_streams(txns, &pool);
    writeln!(io::stdout(), "txns_to_streams done. Elapsed: {:.2?}", now.elapsed())?;


    let accounts = streams_to_accounts(txns_map).await;
    writeln!(io::stdout(), "streams_to_accounts done. Elapsed: {:.2?}", now.elapsed())?;

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
async fn read_txns(path: &std::path::PathBuf) -> io::Result<Vec<Transaction>> {
    let now = std::time::Instant::now();
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .trim(Trim::All)
        .from_path(path)?;
    info!("ReaderBuilder::from_path done. Elapsed: {:.2?}", now.elapsed());

    let now = std::time::Instant::now();
    let all_txns: Vec<Transaction> =
        rdr.deserialize::<Transaction>()
            .filter_map(|record| record.ok())
            .collect();
    info!("reader::deserialize done. Elapsed: {:.2?}", now.elapsed());

    Ok(all_txns)
}

/// Returns a `HashMap` where the key is a `u16` client id,
/// and the value is a `Vec<Transaction>` that
/// belongs to the client.
fn txns_to_map(all_txns: Vec<Transaction>) -> HashMap<u16, Vec<Transaction>> {
    all_txns.into_iter().fold(
        HashMap::new(),
        | mut acc
        , txn: Transaction
        | {
            acc.entry(txn.client_id)
                .or_insert(vec![])
                .push(txn);
            acc
        })
}

/// Returns a `HashMap` where the key is a `u16` client id,
/// and the value is a tuple of
/// `(UnboundedSender<Transaction>, UnboundedReceiver<Transaction>)`
/// produced by `mpsc::unbounded::<Transaction>()`.
fn txns_to_streams(all_txns: Vec<Transaction>, pool: &ThreadPool) -> HashMap<u16, (UnboundedSender<Transaction>, UnboundedReceiver<Transaction>)> {
    let (map, vec) =
        all_txns.into_iter().fold(
            (HashMap::new(), Vec::new()),
            | (mut map, mut vec)
            , txn: Transaction
            | {
                let (tx0, _rx0) =
                    map.entry(txn.client_id)
                        .or_insert(mpsc::unbounded::<Transaction>());
                let tx = tx0.clone();
                let fut = async move {
                    tx.unbounded_send(txn).expect("Failed to send");
                };
                vec.push(fut);
                (map, vec)
            });
    vec.into_iter().for_each(|fut| pool.spawn_ok(fut));
    map
}

/// Reads the `HashMap` in parallel, and returns a list of
/// accounts as `Vec<Account>`.
async fn streams_to_accounts(map: HashMap<u16, (UnboundedSender<Transaction>, UnboundedReceiver<Transaction>)>) -> Vec<Account> {
    let futures = map.into_iter()
        .map(| (client_id, (tx, rx)): (u16, (UnboundedSender<Transaction>, UnboundedReceiver<Transaction>)) | {
            drop(tx);
            stream_to_account(client_id, rx)
        });
    future::join_all(futures).await
}

/// Reads the `HashMap` in parallel, and returns a list of
/// accounts as `Vec<Account>`.
async fn txns_map_to_accounts(txns_map: HashMap<u16, Vec<Transaction>>) -> Vec<Account> {
    txns_map.into_par_iter()
        .map(| (client_id, client_txns) | to_account(client_id, client_txns))
        .collect()
}

/// Reads a sorted list of `Transaction`, and returns an
/// `Account` for a client.
fn to_account(client_id: u16, client_txns: Vec<Transaction>) -> Account {
    let (account, _) =
        client_txns.into_iter().fold(
            (Account::new(client_id), HashMap::new()),
            | (mut account, mut handled): (Account, HashMap<u32, Vec<Transaction>>)
            , txn: Transaction
            | {
                let txn_id = txn.tx_id;
                match handle_txn(&mut account, &handled, &txn) {
                    Ok(()) => handled.entry(txn_id).or_insert(vec![]).push(txn), // only insert when txn ok
                    _ => debug!("Ignoring invalid transaction: {:?}", txn)
                };
                (account, handled)
            }
        );
    account
}

async fn stream_to_account(client_id: u16, rx: UnboundedReceiver<Transaction>) -> Account {
    let (account, _) =
        rx.fold(
            (Account::new(client_id), HashMap::new()),
            | (mut account, mut handled)//: (Account, HashMap<u32, Vec<&Transaction>>)
            ,  txn//: Transaction
            | async move {
                let txn_id = txn.tx_id;
                match handle_txn(&mut account, &handled, &txn) {
                    Ok(()) => handled.entry(txn_id).or_insert(vec![]).push(txn), // only insert when txn ok
                    _ => debug!("Ignoring invalid transaction: {:?}", txn)
                };
                (account, handled)
            })
            .await;
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
    #[ignore]
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
    #[ignore]
    fn test_read_txns() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,1.001
                        withdrawal,2,2,2.0002
                        dispute,3,3,
                        resolve,4,4,
                        chargeback,5,5,
                        bad line
                        chargeback,5,5
                        deposit,1,1,1.0,1.0
                        deposit,x,1,1.0
                        deposit,1,x,1.0
                        deposit,1,1,x")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let txns = block_on(read_txns(&std::path::PathBuf::from(path)))?;

        /*
         * Then
         */
        // txns.sort_by_key(|(i, _)| *i);
        let mut iter = txns.into_iter();
        assert_eq!(iter.next(), Some(Transaction{ kind:      Deposit
                                                , client_id: 1
                                                , tx_id:     1
                                                , amount:    Some(dec!(1.001))
                                                }));
        assert_eq!(iter.next(), Some(Transaction{ kind:      Withdrawal
                                                , client_id: 2
                                                , tx_id:     2
                                                , amount:    Some(dec!(2.0002))
                                                }));
        assert_eq!(iter.next(), Some(Transaction{ kind:      Dispute
                                                , client_id: 3
                                                , tx_id:     3
                                                , amount:    None
                                                }));
        assert_eq!(iter.next(), Some(Transaction{ kind:      Resolve
                                                , client_id: 4
                                                , tx_id:     4
                                                , amount:    None
                                                }));
        assert_eq!(iter.next(), Some(Transaction{ kind: Chargeback
                                                , client_id: 5
                                                , tx_id:     5
                                                , amount:    None
                                                }));
        assert_eq!(iter.next(), None);
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_txns_to_map() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,1.0
                        deposit,2,2,2.0
                        deposit,1,3,2.0
                        withdrawal,1,4,1.5
                        withdrawal,2,5,3.0
                        dispute,4,4,
                        resolve,4,4,
                        chargeback,5,5,
                        bad line
                        deposit,x,x,2.0")?;
        let path = file.path().to_str().unwrap();

        /*
         * When
         */
        let txns = block_on(read_txns(&std::path::PathBuf::from(path)))?;
        let txns_map = txns_to_map(txns);

        /*
         * Then
         */
        // txns_map.iter_mut().for_each(|(_k, v)| v.sort_by_key(|(i, _)| *i) );
        assert_eq!(txns_map.get(&1), Some(&vec![ Transaction{ kind: Deposit, client_id: 1, tx_id: 1, amount: Some(dec!(1.0)) }
                                                , Transaction{ kind: Deposit, client_id: 1, tx_id: 3, amount: Some(dec!(2.0)) }
                                                , Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4, amount: Some(dec!(1.5)) }
                                                ]));
        assert_eq!(txns_map.get(&2), Some(&vec![ Transaction{ kind: Deposit, client_id: 2, tx_id: 2, amount: Some(dec!(2.0)) }
                                                , Transaction{ kind: Withdrawal, client_id: 2, tx_id: 5, amount: Some(dec!(3.0)) }
                                                ]));
        assert_eq!(txns_map.get(&3), None);
        assert_eq!(txns_map.get(&4), Some(&vec![ Transaction{ kind: Dispute, client_id: 4, tx_id: 4, amount: None }
                                                , Transaction{ kind: Resolve, client_id: 4, tx_id: 4, amount: None }
                                                ]));
        assert_eq!(txns_map.get(&5), Some(&vec![ Transaction{ kind: Chargeback, client_id: 5, tx_id: 5, amount: None }
                                                ]));
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_txns_map_to_accounts() {
        /*
         * Given
         */
        let txns =
            hash_map!( 1 => vec![ Transaction{ kind: Deposit,    client_id: 1, tx_id: 1,   amount: Some(dec!(1.0001)) } // +1.0001
                                , Transaction{ kind: Deposit,    client_id: 1, tx_id: 3,   amount: Some(dec!(2.00002)) } // +2.0
                                , Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4,   amount: Some(dec!(1.5001)) } // -1.5001
                                , Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4,   amount: Some(dec!(10.0)) } // ignore
                                , Transaction{ kind: Resolve,    client_id: 1, tx_id: 3,   amount: None } // ignore
                                , Transaction{ kind: Chargeback, client_id: 1, tx_id: 3,   amount: None } // ignore
                                , Transaction{ kind: Dispute,    client_id: 1, tx_id: 3,   amount: None } // hold 2.0
                                , Transaction{ kind: Dispute,    client_id: 1, tx_id: 3,   amount: None } // ignore
                                , Transaction{ kind: Dispute,    client_id: 1, tx_id: 100, amount: None } // ignore
                                , Transaction{ kind: Resolve,    client_id: 1, tx_id: 3,   amount: None } // release 2.0
                                , Transaction{ kind: Dispute,    client_id: 1, tx_id: 4,   amount: None } // hold 1.5001
                                , Transaction{ kind: Chargeback, client_id: 1, tx_id: 4,   amount: None } // revert 1.5001, freeze
                                , Transaction{ kind: Deposit,    client_id: 1, tx_id: 5,   amount: Some(dec!(2.0)) } // ignore
                                ]
                     , 2 => vec![ Transaction{ kind: Deposit,    client_id: 2, tx_id: 101, amount: Some(dec!(5.0)) } // +5.0
                                , Transaction{ kind: Deposit,    client_id: 2, tx_id: 102, amount: Some(dec!(10.0)) } // +10.0
                                , Transaction{ kind: Withdrawal, client_id: 2, tx_id: 103, amount: Some(dec!(1.5)) } // -1.5
                                , Transaction{ kind: Withdrawal, client_id: 2, tx_id: 104, amount: Some(dec!(10.0)) } // -10.0
                                , Transaction{ kind: Resolve,    client_id: 2, tx_id: 103, amount: None } // ignore
                                , Transaction{ kind: Chargeback, client_id: 2, tx_id: 103, amount: None } // ignore
                                , Transaction{ kind: Dispute,    client_id: 2, tx_id: 102, amount: None } // hold 10.0
                                , Transaction{ kind: Dispute,    client_id: 2, tx_id: 101, amount: None } // hold 5.0
                                , Transaction{ kind: Dispute,    client_id: 2, tx_id: 102, amount: None } // ignore
                                , Transaction{ kind: Resolve,    client_id: 2, tx_id: 101, amount: None } // release 5.0
                                , Transaction{ kind: Dispute,    client_id: 2, tx_id: 101, amount: None } // hold 5.0
                                , Transaction{ kind: Chargeback, client_id: 2, tx_id: 102, amount: None } // revert 10.0, freeze
                                , Transaction{ kind: Deposit,    client_id: 2, tx_id: 105, amount: Some(dec!(20.0)) } // ignore
                                ]);
        /*
         * When
         */
        let mut accounts = block_on(txns_map_to_accounts(txns));

        /*
         * Then
         */
        accounts.sort_by_key(|a| a.client_id);
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: dec!(3.0001)
                                          , held:      dec!(0.0)
                                          , total:     dec!(3.0001)
                                          , locked:    true
                                          }
                                 , Account{ client_id: 2
                                          , available: dec!(-11.5)
                                          , held:      dec!(5.0)
                                          , total:     dec!(-6.5)
                                          , locked:    true
                                          }
                                 ]);
    }

    #[test]
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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
    #[ignore]
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