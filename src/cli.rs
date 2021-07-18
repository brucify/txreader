use structopt::StructOpt;

#[derive(Debug)]
#[derive(StructOpt)]
pub struct Cli {
    #[structopt(parse(from_os_str))]
    pub path: std::path::PathBuf,
}

pub fn args() -> Cli {
    Cli::from_args()
}