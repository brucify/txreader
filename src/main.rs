use futures::executor::block_on;
use log::{info, error};
use std::path::PathBuf;
use txreader::cli;
use txreader::tx;

fn main() {
    env_logger::init();
    let args = cli::args();
    if args.generate {
        block_on(generate(args.num_txns, args.num_clients));
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

async fn generate(num_txns: u32, num_clients: u16) {
    info!("Generating {} transactions from {} clients...", num_txns, num_clients);
    tx::generate_txns(num_txns, num_clients).await
}