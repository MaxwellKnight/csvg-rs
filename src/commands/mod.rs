mod csv;
pub mod graph;
mod init;
mod path;

use crate::cli::Commands;
use std::error::Error;

pub fn execute_command(command: &Commands) -> Result<(), Box<dyn Error>> {
    match command {
        Commands::Init(args) => init::execute(args),
        Commands::Csv(args) => csv::execute(args),
        Commands::Graph(args) => graph::execute(args),
        Commands::Path => path::execute(),
    }
}
