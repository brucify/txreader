use std::io::{self, Write};
use std::fs;
use csv::{ReaderBuilder, Trim};
use serde::Deserialize;
use rayon::prelude::*;
use std::sync::mpsc::channel;

#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(rename = "type")]
    kind:       TransactionKind,
    #[serde(rename = "client")]
    client_id:  u16,
    #[serde(rename = "tx")]
    tx_id:      u32,
    amount:     Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "lowercase"))]
enum TransactionKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug)]
struct Account {
    client_id:  u16,
    available:  f64,
    held:       f64,
    total:      f64,
    locked:     bool,
}

pub fn parse_file(path: &std::path::PathBuf) -> io::Result<()> {
    let content = fs::read_to_string(path)?;
    let (tx, rx) = channel::<Transaction>();
    content.lines()
        .collect::<Vec<&str>>()
        .par_iter()
        .for_each_with(tx.clone(), |t, line| {
            match maybe_parse_line(line) {
                None => println!("None"),
                Some(transaction) => {
                    println!("{:?}", transaction);
                    t.send(transaction).unwrap();
                },
            }
        });
    drop(tx);
    rx.iter().for_each(|transaction| {
        match transaction {
            Transaction{kind: TransactionKind::Deposit, ..} => {},
            Transaction{kind: TransactionKind::Withdrawal, ..} => {},
            Transaction{kind: TransactionKind::Dispute, ..} => {},
            Transaction{kind: TransactionKind::Resolve, ..} => {},
            Transaction{kind: TransactionKind::Chargeback, ..} => {},
        };
        writeln!(io::stdout().lock(), "{:?}", transaction).unwrap();
    });
    Ok(())
}

// fn lock_and_writeln(line: &str) -> io::Result<()> {
//     let stdout = io::stdout();
//     let mut handle = stdout.lock();
//     // println!("{}", line);
//     writeln!(handle, "{}", line)
// }

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

#[cfg(test)]
mod test {
    use crate::csv::*;
    use matches::*;

    #[test]
    fn should_open_file() {
        assert_eq!(
            (|| match parse_file(&std::path::PathBuf::from("transactions.csv")) {
                Ok(()) => true,
                _      => false,
            })(),
            true
        )
    }

    #[test]
    fn should_parse_line() {
        let data = "deposit,1,1,1.0";
        assert_matches!(maybe_parse_line(data), Some(Transaction{ kind:      TransactionKind::Deposit
                                                                , client_id: 1
                                                                , tx_id:     1
                                                                , amount:    Some(x)
                                                                }) if x == 1.0);
        let data = "withdrawal,2,2,2.0";
        assert_matches!(maybe_parse_line(data), Some(Transaction{ kind:      TransactionKind::Withdrawal
                                                                , client_id: 2
                                                                , tx_id:     2
                                                                , amount:    Some(x)
                                                                }) if x == 2.0);
        let data = "dispute,3,3,";
        assert_matches!(maybe_parse_line(data), Some(Transaction{ kind:      TransactionKind::Dispute
                                                                , client_id: 3
                                                                , tx_id:     3
                                                                , amount:    None
                                                                }));
        let data = "resolve,4,4,";
        assert_matches!(maybe_parse_line(data), Some(Transaction{ kind:      TransactionKind::Resolve
                                                                , client_id: 4
                                                                , tx_id:     4
                                                                , amount:    None
                                                                }));
        let data = "chargeback,5,5,";
        assert_matches!(maybe_parse_line(data), Some(Transaction{ kind:      TransactionKind::Chargeback
                                                                , client_id: 5
                                                                , tx_id:     5
                                                                , amount:    None
                                                                }));
        let data = "bad line";
        assert_matches!(maybe_parse_line(data), None);
        let data = "chargeback,5,5";
        assert_matches!(maybe_parse_line(data), None);
    }
}