use csvg::{cli, commands};

fn main() {
    let args = cli::parse_args();

    if let Err(e) = commands::execute_command(&args.command) {
        eprintln!("Error: {}", e);
    }
}
