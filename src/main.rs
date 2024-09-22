use csvg::{cli, commands, config};

fn main() {
    let args = cli::parse_args();

    config::redirect_output(args.output).unwrap_or_else(|_| ());
    if let Err(e) = commands::execute_command(&args.command) {
        eprintln!("Error: {}", e);
    }
}
