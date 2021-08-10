use structopt::StructOpt;

#[derive(Debug)]
#[derive(StructOpt)]
pub struct Cli {
    #[structopt(parse(from_os_str), required_unless="generate")]
    pub path: Option<std::path::PathBuf>,

    // Generate a list of random transactions if set to true
    #[structopt(long)]
    pub generate: bool,
}

pub fn args() -> Cli {
    Cli::from_args()
}