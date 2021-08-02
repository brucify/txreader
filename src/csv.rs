use crate::csv::TransactionKind::*;
use csv::{ReaderBuilder, Trim, WriterBuilder};
use log::debug;
use rayon::prelude::*;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Write, Error, ErrorKind::{InvalidInput}};

#[derive(Debug, Deserialize, PartialEq)]
struct Transaction {
    #[serde(rename = "type")]
    kind:       TransactionKind,
    #[serde(rename = "client")]
    client_id:  u16,
    #[serde(rename = "tx")]
    tx_id:      u32,
    amount:     Option<Decimal>,
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

pub fn parse_file(path: &std::path::PathBuf) -> io::Result<()> {
    let txns = read_txns(path)?;
    let txns_map = txns_to_map(txns);
    debug!("Transactions by client: {:?}", txns_map);
    let accounts = txns_map_to_accounts(txns_map);
    print_accounts(&accounts)?;
    Ok(())
}

fn read_txns(path: &std::path::PathBuf) -> io::Result<Vec<(usize, Transaction)>> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .trim(Trim::All)
        .from_path(path)?;

    // deserialize lines in file in parallel,
    // while keeping the original index
    let all_txns: Vec<(usize, Transaction)> =
        rdr.deserialize::<Transaction>()
            .enumerate()
            .par_bridge()
            .filter_map(|(i, record)| {
                record.map_or(None, |transaction| Some((i, transaction)))
            })
            .collect();

    Ok(all_txns)
}

fn txns_to_map(all_txns: Vec<(usize, Transaction)>) -> HashMap<u16, Vec<(usize, Transaction)>> {
    all_txns.into_iter().fold(
        HashMap::new(),
        | mut acc
        , (i, txn): (usize, Transaction)
        | {
            acc.entry(txn.client_id)
                .or_insert(vec![])
                .push((i, txn));
            acc
        })
}

fn txns_map_to_accounts(txns_map: HashMap<u16, Vec<(usize, Transaction)>>) -> Vec<Account> {
    txns_map.into_par_iter()
        .map(| (client_id, mut client_txns) | {
            client_txns.par_sort_by_key(|(i, _)| *i); // client_txns is unordered due to parallel deserialization
            to_account(client_id, client_txns)
        })
        .collect()
}

fn to_account(client_id: u16, client_txns: Vec<(usize, Transaction)>) -> Account {
    let (account, _) =
        client_txns.iter().fold(
            (Account::new(client_id), HashMap::new()),
            | (mut account, mut handled): (Account, HashMap<u32, Vec<&Transaction>>)
            , (_i, txn): &(usize, Transaction)
            | {
                match handle_txn(&mut account, &handled, txn) {
                    Ok(()) => handled.entry(txn.tx_id).or_insert(vec![]).push(&txn), // only insert when txn ok
                    _ => debug!("Invalid transaction: {:?}", txn)
                };
                (account, handled)
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
            // A deposit is a credit to the client's asset account,
            // meaning it should increase the available and total
            // funds of the client account
            account.available += amount;
            account.total     += amount;
            Ok(())
        },
        &Transaction{ kind: Withdrawal, amount: Some(amount), .. } => {
            // If a client does not have sufficient available funds
            // the withdrawal should fail and the total amount of
            // funds should not change
            (!account.locked && account.available >= amount).then(|| ())
                .ok_or(Error::from(InvalidInput))?;
            // A withdraw is a debit to the client's asset account,
            // meaning it should decrease the available and total
            // funds of the client account
            account.available -= amount;
            account.total     -= amount;
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
                (false, Some(&&Transaction{ kind: Deposit, amount: Some(amount), .. })) => {
                    // A dispute represents a client's claim that a
                    // transaction was erroneous and should be reversed.
                    // The transaction shouldn't be reversed yet but
                    // the associated funds should be held. This means
                    // that the clients available funds should decrease
                    // by the amount disputed, their held funds should
                    // increase by the amount disputed, while their
                    // total funds should remain the same.
                    account.available -= amount;
                    account.held      += amount;
                    Ok(())
                },
                (false, Some(&&Transaction{ kind: Withdrawal, amount: Some(amount), .. })) => {
                    // NOTE: Assumes a dispute on a withdrawal temporarily
                    // puts funds into the client's held funds.
                    account.held      += amount;
                    account.total     += amount;
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
                (true, Some(&&Transaction{ kind: Deposit, amount: Some(amount), .. })) => {
                    // A resolve represents a resolution to a dispute,
                    // releasing the associated held funds. Funds that
                    // were previously disputed are no longer disputed.
                    // This means that the clients held funds should
                    // decrease by the amount no longer disputed, their
                    // available funds should increase by the amount no
                    // longer disputed, and their total funds should
                    // remain the same.
                    account.available += amount;
                    account.held      -= amount;
                    Ok(())
                },
                (true, Some(&&Transaction{ kind: Withdrawal, amount: Some(amount), .. })) => {
                    // NOTE: Assumes a resolve removes the temporarily
                    // increased funds from the client's held funds.
                    account.held      -= amount;
                    account.total     -= amount;
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
                (true, Some(&&Transaction{ kind: Deposit, amount: Some(amount), .. })) => {
                    // A chargeback is the final state of a dispute and
                    // represents the client reversing a transaction.
                    // Funds that were held have now been withdrawn.
                    // This means that the clients held funds and total
                    // funds should decrease by the amount previously
                    // disputed. If a chargeback occurs the client's
                    // account should be immediately frozen.
                    account.held   -= amount;
                    account.total  -= amount;
                    account.locked  = true;
                    Ok(())
                },
                (true, Some(&&Transaction{ kind: Withdrawal, amount: Some(amount), .. })) => {
                    // NOTE: Assumes a chargeback to a withdrawal reverses
                    // a withdrawal, and puts the temporarily held funds
                    // back to the client available funds.
                    account.available += amount;
                    account.held      -= amount;
                    account.locked     = true;
                    Ok(())
                },
                _ => Err(Error::from(InvalidInput))
            }
        },
        _ => Err(Error::from(InvalidInput))
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

fn print_accounts(accounts: &Vec<Account>) -> io::Result<()>{
    writeln!(io::stdout().lock(), "client_id,available,held,total,locked")?;
    accounts.par_iter().for_each(|account| maybe_print_account(account));
    Ok(())
}

fn maybe_print_account(account: &Account) {
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_writer(vec![]);
    wtr.serialize(account).unwrap();
    let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    write!(io::stdout().lock(), "{}", data).unwrap();
}

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
    fn test_read_txns_map() -> Result<(), Box<dyn std::error::Error>> {
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
        let txns = read_txns(&std::path::PathBuf::from(path))?;
        let mut txns_map = txns_to_map(txns);

        /*
         * Then
         */
        txns_map.iter_mut().for_each(|(_k, v)| v.sort_by_key(|(i, _)| *i) );
        assert_eq!(txns_map.get(&1), Some(&vec![ (0, Transaction{ kind: Deposit, client_id: 1, tx_id: 1, amount: Some(dec!(1.0)) })
                                                , (2, Transaction{ kind: Deposit, client_id: 1, tx_id: 3, amount: Some(dec!(2.0)) })
                                                , (3, Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4, amount: Some(dec!(1.5)) })
                                                ]));
        assert_eq!(txns_map.get(&2), Some(&vec![ (1, Transaction{ kind: Deposit, client_id: 2, tx_id: 2, amount: Some(dec!(2.0)) })
                                                , (4, Transaction{ kind: Withdrawal, client_id: 2, tx_id: 5, amount: Some(dec!(3.0)) })
                                                ]));
        assert_eq!(txns_map.get(&3), None);
        assert_eq!(txns_map.get(&4), Some(&vec![ (5, Transaction{ kind: Dispute, client_id: 4, tx_id: 4, amount: None })
                                                , (6, Transaction{ kind: Resolve, client_id: 4, tx_id: 4, amount: None })
                                                ]));
        assert_eq!(txns_map.get(&5), Some(&vec![ (7, Transaction{ kind: Chargeback, client_id: 5, tx_id: 5, amount: None })
                                                ]));
        Ok(())
    }

    #[test]
    fn test_txns_map_to_accounts() {
        /*
         * Given
         */
        let txns =
            hash_map!( 1 => vec![ (1,  Transaction{ kind: Deposit,    client_id: 1, tx_id: 1,   amount: Some(dec!(1.00001)) }) // +1
                                , (3,  Transaction{ kind: Deposit,    client_id: 1, tx_id: 3,   amount: Some(dec!(2.0)) }) // +2
                                , (4,  Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4,   amount: Some(dec!(1.5)) }) // -1.5
                                , (5,  Transaction{ kind: Withdrawal, client_id: 1, tx_id: 4,   amount: Some(dec!(10.0)) }) // ignore
                                , (6,  Transaction{ kind: Resolve,    client_id: 1, tx_id: 3,   amount: None }) // ignore
                                , (6,  Transaction{ kind: Chargeback, client_id: 1, tx_id: 3,   amount: None }) // ignore
                                , (7,  Transaction{ kind: Dispute,    client_id: 1, tx_id: 3,   amount: None }) // hold 2
                                , (8,  Transaction{ kind: Dispute,    client_id: 1, tx_id: 3,   amount: None }) // ignore
                                , (9,  Transaction{ kind: Dispute,    client_id: 1, tx_id: 100, amount: None }) // ignore
                                , (10, Transaction{ kind: Resolve,    client_id: 1, tx_id: 3,   amount: None }) // release 2
                                , (11, Transaction{ kind: Dispute,    client_id: 1, tx_id: 4,   amount: None }) // hold 1.5
                                , (12, Transaction{ kind: Chargeback, client_id: 1, tx_id: 4,   amount: None }) // revert 1.5, freeze
                                , (13, Transaction{ kind: Deposit,    client_id: 1, tx_id: 5,   amount: Some(dec!(2.0)) }) // ignore
                                ]
                     , 2 => vec![ (14, Transaction{ kind: Deposit,    client_id: 2, tx_id: 101, amount: Some(dec!(5.0)) }) // +5
                                , (15, Transaction{ kind: Deposit,    client_id: 2, tx_id: 102, amount: Some(dec!(10.0)) }) // +10
                                , (16, Transaction{ kind: Withdrawal, client_id: 2, tx_id: 103, amount: Some(dec!(1.5)) }) // -1.5
                                , (17, Transaction{ kind: Withdrawal, client_id: 2, tx_id: 104, amount: Some(dec!(10.0)) }) // -10
                                , (18, Transaction{ kind: Resolve,    client_id: 2, tx_id: 103, amount: None }) // ignore
                                , (19, Transaction{ kind: Chargeback, client_id: 2, tx_id: 103, amount: None }) // ignore
                                , (20, Transaction{ kind: Dispute,    client_id: 2, tx_id: 102, amount: None }) // hold 10
                                , (21, Transaction{ kind: Dispute,    client_id: 2, tx_id: 101, amount: None }) // hold 5
                                , (22, Transaction{ kind: Dispute,    client_id: 2, tx_id: 102, amount: None }) // ignore
                                , (23, Transaction{ kind: Resolve,    client_id: 2, tx_id: 101, amount: None }) // release 5
                                , (24, Transaction{ kind: Dispute,    client_id: 2, tx_id: 101, amount: None }) // hold 5
                                , (25, Transaction{ kind: Chargeback, client_id: 2, tx_id: 102, amount: None }) // revert 10, freeze
                                , (26, Transaction{ kind: Deposit,    client_id: 2, tx_id: 105, amount: Some(dec!(20.0)) }) // ignore
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
                                          , available: dec!(3.00001)
                                          , held:      dec!(0.0)
                                          , total:     dec!(3.00001)
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
    fn test_read_txns() -> Result<(), Box<dyn std::error::Error>> {
        /*
         * Given
         */
        let mut file = NamedTempFile::new()?;
        writeln!(file, "type,client,tx,amount
                        deposit,1,1,1.0001
                        withdrawal,2,2,2.0
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
        let mut txns = read_txns(&std::path::PathBuf::from(path))?;

        /*
         * Then
         */
        txns.sort_by_key(|(i, _)| *i);
        let mut iter = txns.into_iter();
        assert_eq!(iter.next(), Some((0, Transaction{ kind:      Deposit
                                                    , client_id: 1
                                                    , tx_id:     1
                                                    , amount:    Some(dec!(1.0001))
                                                    })));
        assert_eq!(iter.next(), Some((1, Transaction{ kind:      Withdrawal
                                                    , client_id: 2
                                                    , tx_id:     2
                                                    , amount:    Some(dec!(2.0))
                                                    })));
        assert_eq!(iter.next(), Some((2, Transaction{ kind:      Dispute
                                                    , client_id: 3
                                                    , tx_id:     3
                                                    , amount:    None
                                                    })));
        assert_eq!(iter.next(), Some((3, Transaction{ kind:      Resolve
                                                    , client_id: 4
                                                    , tx_id:     4
                                                    , amount:    None
                                                    })));
        assert_eq!(iter.next(), Some((4, Transaction{ kind: Chargeback
                                                    , client_id: 5
                                                    , tx_id:     5
                                                    , amount:    None
                                                    })));
        assert_eq!(iter.next(), None);
        Ok(())
    }

}