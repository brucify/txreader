use crate::csv::TransactionKind::*;
use csv::{ReaderBuilder, Trim};
use log::debug;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write, Error, ErrorKind::{InvalidInput}};
use std::sync::mpsc::channel;

#[derive(Debug, Deserialize, PartialEq)]
struct Transaction {
    #[serde(rename = "type")]
    kind:       TransactionKind,
    #[serde(rename = "client")]
    client_id:  u16,
    #[serde(rename = "tx")]
    tx_id:      u32,
    amount:     Option<f64>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all(deserialize = "lowercase"))]
enum TransactionKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Serialize, PartialEq)]
struct Account {
    client_id:  u16,
    available:  f64,
    held:       f64,
    total:      f64,
    locked:     bool,
}

impl Account {
    fn new(client_id: u16) -> Account {
        Account {
            client_id,
            available: 0.0,
            held:      0.0,
            total:     0.0,
            locked:    false
        }
    }
}

pub fn parse_file(path: &std::path::PathBuf) -> io::Result<()> {
    let txns_map = file_to_txns_map(path)?;
    debug!("txns: {:?}", txns_map);
    let accounts = txns_map_to_accounts(txns_map);
    accounts.par_iter().for_each(|a|
        writeln!(io::stdout().lock(), "account: {:?}", a).unwrap()
    );
    Ok(())
}

fn file_to_txns_map(path: &std::path::PathBuf) -> io::Result<HashMap<u16, Vec<(usize, Transaction)>>> {
    let content = fs::read_to_string(path)?;
    let valid_lines: Vec<Transaction> =
        content.par_lines()
            .filter_map(|line| maybe_parse_line(line))
            .collect();

    let txns_map: HashMap<u16, Vec<(usize, Transaction)>> =
        valid_lines.into_iter()
            .enumerate()
            .fold(
                HashMap::new() as HashMap<u16, Vec<(usize, Transaction)>>,
                | mut accounts
                , (i, txn): (usize, Transaction)
                | {
                    accounts.entry(txn.client_id)
                        .or_insert(vec![])
                        .push((i, txn));
                    accounts
                });
    Ok(txns_map)
}

fn maybe_parse_line(data: &str) -> Option<Transaction> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .trim(Trim::All)
        .from_reader(data.as_bytes());

    match rdr.deserialize().next() {
        Some(Ok(transaction)) => Some(transaction),
        _ => None,
    }
}

fn txns_map_to_accounts(txns_map: HashMap<u16, Vec<(usize, Transaction)>>) -> Vec<Account> {
    let (tx0, rx0) = channel::<Account>();
    txns_map.into_par_iter()
        .for_each_with(tx0.clone(),
            | tx, (client_id, mut client_txns): (u16, Vec<(usize, Transaction)>) | {
                client_txns.par_sort_by_key(|(i, _)| *i);
                let account = to_account(client_id, client_txns);
                tx.send(account).unwrap();
            });
    drop(tx0);

    rx0.iter().collect::<Vec<Account>>()
}

fn to_account(client_id: u16, client_txns: Vec<(usize, Transaction)>) -> Account {
    let (_, account) = client_txns.iter()
        .fold(
            (HashMap::new(), Account::new(client_id)),
            | (mut handled, mut account): (HashMap<u32, Vec<&Transaction>>, Account)
            , (_i, txn): &(usize, Transaction)
            | {
                if let Ok(()) = handle_txn(&mut account, &handled, txn) {
                    handled.entry(txn.tx_id).or_insert(vec![]).push(&txn); // only insert when txn ok
                };
                (handled, account)
            }
        );
    account
}

fn handle_txn( account: &mut Account
             , handled: &HashMap<u32, Vec<&Transaction>>
             , txn:     &Transaction
             ) -> io::Result<()> {
    match txn {
        &Transaction{ kind: Deposit, amount: Some(amount), .. } => {
            (!account.locked).then(|| ())
                .ok_or(Error::from(InvalidInput))?;
            account.available += amount;
            account.total     += amount;
            Ok(())
        },
        &Transaction{ kind: Withdrawal, amount: Some(amount), .. } => {
            (!account.locked && account.available >= amount).then(|| ())
                .ok_or(Error::from(InvalidInput))?;
            account.available -= amount;
            account.total     -= amount;
            Ok(())
        },
        &Transaction{ kind: Dispute, tx_id, .. } => {
            let txns = handled.get(&tx_id)
                .ok_or(Error::from(InvalidInput))?;
            let dispute = is_under_dispute(txns);
            let t = initial_txn(txns);
            match (dispute, t) {
                (false, Some(&Transaction{ amount: Some(amount), .. })) => {
                    account.available -= amount;
                    account.held      += amount;
                    Ok(())
                },
                _ => {
                    debug!("Not Done: {:?}", txn);
                    Err(Error::from(InvalidInput))
                }
            }
        },
        &Transaction{ kind: Resolve, tx_id, .. } => {
            let txns = handled.get(&tx_id)
                .ok_or(Error::from(InvalidInput))?;
            let dispute = is_under_dispute(txns);
            let t = initial_txn(txns);
            match (dispute, t) {
                (true, Some(&Transaction{ amount: Some(amount), .. })) => {
                    account.available += amount;
                    account.held      -= amount;
                    Ok(())
                },
                _ => {
                    debug!("Not Done: {:?}", txn);
                    Err(Error::from(InvalidInput))
                }
            }
        },
        &Transaction{ kind: Chargeback, tx_id, .. } => {
            let txns = handled.get(&tx_id)
                .ok_or(Error::from(InvalidInput))?;
            let dispute = is_under_dispute(txns);
            let t = initial_txn(txns);
            match (dispute, t) {
                (true, Some(&Transaction{ kind: Deposit, amount: Some(amount), .. })) => {
                    account.held   -= amount;
                    account.total  -= amount;
                    account.locked  = true;
                    Ok(())
                },
                (true, Some(&Transaction{ kind: Withdrawal, amount: Some(amount), .. })) => {
                    account.held   -= amount;
                    account.total  += amount;
                    account.locked  = true;
                    Ok(())
                },
                _ => {
                    debug!("Not Done: {:?}", txn);
                    Err(Error::from(InvalidInput))
                }
            }
        },
        _ => {
            debug!("Not Done: {:?}", txn);
            Err(Error::from(InvalidInput))
        }
    }
}

fn is_under_dispute(txns: &Vec<&Transaction>) -> bool {
    let n_dispute = txns.iter().filter(|t| t.kind == Dispute).count();
    let n_resolve = txns.iter().filter(|t| t.kind == Resolve).count();
    n_dispute > n_resolve
}

fn initial_txn<'a>(txns: &'a Vec<&'a Transaction>) -> Option<&'a &Transaction> {
    txns.iter().filter(|t| t.kind == Withdrawal || t.kind == Deposit).next()
}

// fn lock_and_writeln(line: &str) -> io::Result<()> {
//     let stdout = io::stdout();
//     let mut handle = stdout.lock();
//     // debug!("{}", line);
//     writeln!(handle, "{}", line)
// }

#[cfg(test)]
mod test {
    use common_macros::hash_map;
    use crate::csv::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_file() -> io::Result<()> {
        assert_eq!(parse_file(&std::path::PathBuf::from("transactions.csv"))?, ());
        Ok(())
    }

    #[test]
    fn test_file_to_txns() -> Result<(), Box<dyn std::error::Error>> {
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
        let mut txns = file_to_txns_map(&std::path::PathBuf::from(path)).unwrap();

        /*
         * Then
         */
        txns.iter_mut().for_each(|(_k, v)| v.sort_by_key(|(i, _)| *i) );
        assert_eq!(txns.get(&1), Some(&vec![ (0, Transaction{ kind: Deposit, client_id: 1, tx_id: 1, amount: Some(1.0) })
                                             , (2, Transaction{ kind: Deposit, client_id: 1, tx_id: 3, amount: Some(2.0) })
                                             , (3, Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4, amount: Some(1.5) })
                                             ]));
        assert_eq!(txns.get(&2), Some(&vec![ (1, Transaction{ kind: Deposit, client_id: 2, tx_id: 2, amount: Some(2.0) })
                                             , (4, Transaction{ kind: Withdrawal, client_id: 2, tx_id: 5, amount: Some(3.0) })
                                             ]));
        assert_eq!(txns.get(&3), None);
        assert_eq!(txns.get(&4), Some(&vec![ (5, Transaction{ kind: Dispute, client_id: 4, tx_id: 4, amount: None })
                                             , (6, Transaction{ kind: Resolve, client_id: 4, tx_id: 4, amount: None })
                                             ]));
        assert_eq!(txns.get(&5), Some(&vec![ (7, Transaction{ kind: Chargeback, client_id: 5, tx_id: 5, amount: None })
                                             ]));
        Ok(())
    }

    #[test]
    fn test_txns_to_accounts() {
        /*
         * Given
         */
        let txns =
            hash_map!( 1 => vec![ (1,  Transaction{ kind: Deposit,    client_id: 1, tx_id: 1,   amount: Some(1.0) }) // +1
                                , (3,  Transaction{ kind: Deposit,    client_id: 1, tx_id: 3,   amount: Some(2.0) }) // +2
                                , (4,  Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4,   amount: Some(1.5) }) // -1.5
                                , (5,  Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4,   amount: Some(10.0) }) // ignore
                                , (6,  Transaction{ kind: Resolve,    client_id: 1, tx_id: 3,   amount: None }) // ignore
                                , (6,  Transaction{ kind: Chargeback, client_id: 1, tx_id: 3,   amount: None }) // ignore
                                , (7,  Transaction{ kind: Dispute,    client_id: 1, tx_id: 3,   amount: None }) // hold 2
                                , (8,  Transaction{ kind: Dispute,    client_id: 1, tx_id: 3,   amount: None }) // ignore
                                , (9,  Transaction{ kind: Dispute,    client_id: 1, tx_id: 100, amount: None }) // ignore
                                , (10, Transaction{ kind: Resolve,    client_id: 1, tx_id: 3,   amount: None }) // release 2
                                , (11, Transaction{ kind: Dispute,    client_id: 1, tx_id: 4,   amount: None }) // hold 1.5
                                , (12, Transaction{ kind: Chargeback, client_id: 1, tx_id: 4,   amount: None }) // revert 1.5, freeze
                                , (13, Transaction{ kind: Deposit,    client_id: 1, tx_id: 5,   amount: Some(2.0) }) // ignore
                                ]
                     , 2 => vec![ (14, Transaction{ kind: Deposit,    client_id: 2, tx_id: 101, amount: Some(5.0) }) // +5
                                , (15, Transaction{ kind: Deposit,    client_id: 2, tx_id: 102, amount: Some(10.0) }) // +10
                                , (16, Transaction{ kind: Withdrawal, client_id: 2, tx_id: 103, amount: Some(1.5) }) // -1.5
                                , (17, Transaction{ kind: Withdrawal, client_id: 2, tx_id: 104, amount: Some(10.0) }) // -10
                                , (18, Transaction{ kind: Resolve,    client_id: 2, tx_id: 103, amount: None }) // ignore
                                , (19, Transaction{ kind: Chargeback, client_id: 2, tx_id: 103, amount: None }) // ignore
                                , (20, Transaction{ kind: Dispute,    client_id: 2, tx_id: 102, amount: None }) // hold 10
                                , (21, Transaction{ kind: Dispute,    client_id: 2, tx_id: 101, amount: None }) // hold 5
                                , (22, Transaction{ kind: Dispute,    client_id: 2, tx_id: 102, amount: None }) // ignore
                                , (23, Transaction{ kind: Resolve,    client_id: 2, tx_id: 101, amount: None }) // release 5
                                , (24, Transaction{ kind: Dispute,    client_id: 2, tx_id: 101, amount: None }) // hold 5
                                , (25, Transaction{ kind: Chargeback, client_id: 2, tx_id: 102, amount: None }) // revert 10, freeze
                                , (26, Transaction{ kind: Deposit,    client_id: 2, tx_id: 105, amount: Some(20.0) }) // ignore
                                ]);
        /*
         * When
         */
        let mut accounts = txns_map_to_accounts(txns);

        /*
         * Then
         */
        accounts.sort_by_key(|a| a.client_id);
        assert_eq!(accounts, vec![ Account{ client_id: 1
                                          , available: 0.0
                                          , held:      0.0
                                          , total:     3.0
                                          , locked:    true
                                          }
                                 , Account{ client_id: 2
                                          , available: -11.5
                                          , held:      5.0
                                          , total:     -6.5
                                          , locked:    true
                                          }
                                 ]);
    }

    #[test]
    fn test_maybe_parse_line() {
        let data = "deposit,1,1,1.0";
        assert_eq!(maybe_parse_line(data), Some(Transaction{ kind:      Deposit
                                                           , client_id: 1
                                                           , tx_id:     1
                                                           , amount:    Some(1.0)
                                                           }));
        let data = "withdrawal,2,2,2.0";
        assert_eq!(maybe_parse_line(data), Some(Transaction{ kind:      Withdrawal
                                                           , client_id: 2
                                                           , tx_id:     2
                                                           , amount:    Some(2.0)
                                                           }));
        let data = "dispute,3,3,";
        assert_eq!(maybe_parse_line(data), Some(Transaction{ kind:      Dispute
                                                           , client_id: 3
                                                           , tx_id:     3
                                                           , amount:    None
                                                           }));
        let data = "resolve,4,4,";
        assert_eq!(maybe_parse_line(data), Some(Transaction{ kind:      Resolve
                                                           , client_id: 4
                                                           , tx_id:     4
                                                           , amount:    None
                                                           }));
        let data = "chargeback,5,5,";
        assert_eq!(maybe_parse_line(data), Some(Transaction{ kind:      Chargeback
                                                           , client_id: 5
                                                           , tx_id:     5
                                                           , amount:    None
                                                           }));
        let data = "bad line";
        assert_eq!(maybe_parse_line(data), None);
        let data = "chargeback,5,5";
        assert_eq!(maybe_parse_line(data), None);
        // let data = "deposit,1,1,1.0,1.0";
        // assert_eq!(maybe_parse_line(data), None);
        let data = "deposit,x,1,1.0";
        assert_eq!(maybe_parse_line(data), None);
        let data = "deposit,1,x,1.0";
        assert_eq!(maybe_parse_line(data), None);
        let data = "deposit,1,1,x";
        assert_eq!(maybe_parse_line(data), None);
    }

}