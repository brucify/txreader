use futures::executor::block_on;
use log::{info, error};
use txreader::tx;
use txreader::cli;
use std::path::PathBuf;

fn main() {
    env_logger::init();
    let args = cli::args();
    if args.generate {
        block_on(generate());
    } else {
        block_on(read(&args.path.unwrap()));
    }
}

async fn read(path: &PathBuf) {
    info!("Reading from path {:?}", path);
    match tx::read(path).await {
        Ok(_) => info!("Done."),
        Err(error) => error!("Error: {:?}", error)
    }
}

async fn generate() {
    let num_txns = 10000;
    let num_clients = 100;
    info!("Generating {} transactions from {} clients...", num_txns, num_clients);
    tx::generate_txns(num_txns, num_clients).await
}