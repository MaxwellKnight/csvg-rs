use crate::config;
use std::error::Error;

/// Executes the path command which displays the config's directory
pub fn execute() -> Result<(), Box<dyn Error>> {
    let config_dir = config::create_config_folder()?;
    println!("config file is located here:\n\t{}", config_dir.display());
    Ok(())
}
