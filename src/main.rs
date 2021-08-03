use txreader::cli;
use txreader::csv;
use anyhow::Context;
use log::{info, error};

fn main() {
    run()
}

fn run() {
    env_logger::init();
    let args = cli::args();
    info!("Reading from path {:?}", args.path);
    match csv::parse_file(&args.path)
        .with_context(|| format!("Could not read file `{:?}`", &args.path)) {
        Ok(_) => info!("Done."),
        Err(error) => error!("Error: {:?}", error)
    }
}
