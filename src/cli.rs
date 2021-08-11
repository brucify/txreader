use structopt::StructOpt;

#[derive(Debug)]
#[derive(StructOpt)]
pub struct Cli {
    #[structopt(parse(from_os_str), required_unless="generate", help = "Path to the csv file that contains transactions. Optional if --generate is set")]
    pub path: Option<std::path::PathBuf>,

    // Generate a list of random transactions if set to true
    #[structopt(short = "G", long = "generate", help = "Generates a list of random transactions")]
    pub generate: bool,

    #[structopt(short = "t", long = "transactions", default_value = "10000", help = "Number of transactions to generate")]
    pub num_txns: u32,

    #[structopt(short = "c", long = "clients", default_value = "100", help = "Number of clients in the generated transactions")]
    pub num_clients: u16,
}

pub fn args() -> Cli {
    Cli::from_args()
}