use txreader::cli;
use txreader::csv;
use anyhow::Context;

fn main() {
    run()
}

fn run() {
    let args = cli::args();
    let result = csv::parse_file(&args.path)
        .with_context(|| format!("Could not read file `{:?}`", &args.path));
    match result {
        Ok(_) => { println!("Done.") }
        Err(error) => { println!("Error: {:?}", error) }
    }
}
