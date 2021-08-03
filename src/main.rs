use futures::executor::block_on;
use log::{info, error};
use txreader::tx;
use txreader::cli;

fn main() {
    run()
}

fn run() {
    env_logger::init();
    let args = cli::args();
    info!("Reading from path {:?}", args.path);
    let fut = tx::from_path(&args.path);
    match block_on(fut) {
        Ok(_) => info!("Done."),
        Err(error) => error!("Error: {:?}", error)
    }
}
