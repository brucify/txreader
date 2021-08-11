use structopt::StructOpt;

#[derive(Debug)]
#[derive(StructOpt)]
pub struct Cli {
    #[structopt(parse(from_os_str), required_unless="generate")]
    pub path: Option<std::path::PathBuf>,

    // Generate a list of random transactions if set to true
    #[structopt(long)]
    pub generate: bool,

    #[structopt(short = "t", long = "transactions", default_value = "10000")]
    pub num_txns: u32,

    #[structopt(short = "c", long = "clients", default_value = "100")]
    pub num_clients: u16,
}

pub fn args() -> Cli {
    Cli::from_args()
}