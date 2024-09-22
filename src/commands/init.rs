use crate::cli::InitArgs;
use crate::config::{self, Config};
use crate::sql::process_sql_schema;
use std::error::Error;
use std::path::Path;
use std::process::exit;

/// Execute initialization of config and default settings
pub fn execute(args: &InitArgs) -> Result<(), Box<dyn Error>> {
    let config_path = Path::new(".csvgraph/config.json");
    if config_path.exists() && !args.force {
        println!(
            "Config file already exists at {}. Use --force to overwrite.",
            config::display_relative_path(config_path)
        );
        return Ok(());
    }

    let config_dir = config::create_config_folder().unwrap_or_else(|e| {
        eprintln!("Failed to create config folder: {}", e);
        exit(1);
    });

    let config_file = config_dir.join("config.json");
    let config = Config::default();

    config::write_config(&config, &config_file).unwrap_or_else(|e| {
        eprintln!("Failed to write config file: {}", e);
        exit(1);
    });

    println!(
        "Configuration file created successfully at {}",
        config::display_relative_path(&config_file)
    );

    if let Some(schema_path) = config::find_sql_schema() {
        println!(
            "Found SQL schema: {}",
            config::display_relative_path(&schema_path)
        );
        process_sql_schema(&schema_path, &config_dir).unwrap_or_else(|e| {
            eprintln!("Failed to process SQL schema: {}", e);
            eprintln!("The configuration was created, but the SQL schema could not be processed.");
            exit(1);
        });
        println!("SQL schema processed successfully.");
    } else {
        println!("No SQL schema found in the current directory.");
    }

    println!("Configuration initialized successfully in the current working directory.");
    Ok(())
}
