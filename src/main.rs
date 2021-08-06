use futures::executor::block_on;
use log::{info, error};
use txreader::tx;
use txreader::cli;

fn main() {
    block_on(run())
}

async fn run() {
    env_logger::init();
    let args = cli::args();
    info!("Reading from path {:?}", args.path);
    match tx::read(&args.path).await {
        Ok(_) => info!("Done."),
        Err(error) => error!("Error: {:?}", error)
    }
}