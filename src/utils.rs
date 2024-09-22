use std::io::{self, IsTerminal};

pub fn is_pipe() -> bool {
    !io::stdout().is_terminal()
}

pub fn print_info(msg: &str) {
    if !is_pipe() {
        println!("{}", msg);
    }
}
