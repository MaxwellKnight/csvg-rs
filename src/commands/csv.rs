use crate::cli::{CsvArgs, CsvSubcommands, JoinType};
use crate::config::{create_config_folder, read_config, Config};
use crate::csv::{self, DataFrame};
use crate::utils::print_info;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read};
use std::path::Path;

/// Execute CSV operations based on command line arguments.
pub fn execute(args: &CsvArgs) -> Result<(), Box<dyn Error>> {
    let config_dir = create_config_folder()?;
    let config: Config = read_config(&config_dir)?;

    match &args.subcommand {
        CsvSubcommands::Head { file, lines } => handle_head(&config, file, *lines),
        CsvSubcommands::Tail { file, lines } => handle_tail(&config, file, *lines),
        CsvSubcommands::Concat { files } => handle_concat(&config, files),
        CsvSubcommands::Drop { file, columns } => handle_drop(&config, file, columns),
        CsvSubcommands::Select { file, columns } => handle_select(&config, file, columns),
        CsvSubcommands::Join {
            file1,
            file2,
            left_column,
            right_column,
            r#type,
        } => handle_join(&config, file1, file2, left_column, right_column, r#type),
    }
}

/// Display the first n lines of a CSV file.
fn handle_head(config: &Config, file: &str, lines: usize) -> Result<(), Box<dyn Error>> {
    let file_path = config.source_path.join(format!("{}.csv", file));
    println!("{:?}", file_path);
    csv::read_csv_stream(&file_path, Some(lines), false)?;
    print_info(&format!(
        "Successfully displayed first {} lines from '{}'",
        lines, file
    ));
    Ok(())
}

/// Display the last n lines of a CSV file.
fn handle_tail(config: &Config, file: &str, lines: usize) -> Result<(), Box<dyn Error>> {
    let file_path = config.source_path.join(format!("{}.csv", file));
    csv::read_csv_stream(&file_path, Some(lines), true)?;
    print_info(&format!(
        "Successfully displayed last {} lines from '{}'",
        lines, file
    ));
    Ok(())
}

/// Concatenate multiple CSV files.
fn handle_concat(config: &Config, files: &[String]) -> Result<(), Box<dyn Error>> {
    if files.len() < 2 {
        eprintln!("Error: At least two files are needed to use the concat command");
        return Ok(());
    }

    let mut df = DataFrame::new("concatenated".to_string());
    df.read_csv_stream(Path::new(&files[0]))?;
    let stdout = io::stdout();

    let mut writer = BufWriter::new(stdout.lock());
    df.write_csv_stream(&mut writer)?;

    for file in files {
        let file = config.source_path.join(format!("{}.csv", file));
        let mut input = BufReader::new(File::open(file)?);
        df.concat_stream(&mut input.by_ref(), &mut writer)?;
    }
    print_info(&format!("Successfully concatenated {} files", files.len()));
    Ok(())
}

/// Drop specified columns from a CSV file.
fn handle_drop(config: &Config, file: &str, columns: &[String]) -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new(file.to_string());
    let file = config.source_path.join(format!("{}.csv", file));
    df.read_csv_stream(&file)?;

    let mut input = BufReader::new(File::open(file.clone())?);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    df.drop_stream(&mut input.by_ref(), &mut writer, columns)?;
    print_info(&format!(
        "Successfully dropped columns {:?} from '{:?}'",
        columns, file
    ));
    Ok(())
}

/// Select specified columns from a CSV file.
fn handle_select(config: &Config, file: &str, columns: &[String]) -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new(file.to_string());
    let file = config.source_path.join(format!("{}.csv", file));
    df.read_csv_stream(Path::new(&file.clone()))?;

    let mut input = BufReader::new(File::open(file.clone())?);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    df.select_stream(&mut input.by_ref(), &mut writer, columns)?;
    print_info(&format!(
        "Successfully selected columns {:?} from '{:?}'",
        columns, file
    ));
    Ok(())
}

/// Join two CSV files based on specified columns.
fn handle_join(
    config: &Config,
    file1: &str,
    file2: &str,
    left_column: &str,
    right_column: &str,
    r#type: &JoinType,
) -> Result<(), Box<dyn Error>> {
    let mut left_df = DataFrame::new(file1.to_string());
    let file1 = config.source_path.join(format!("{}.csv", file1));
    left_df.read_csv_stream(&file1)?;

    let file2 = config.source_path.join(format!("{}.csv", file2));
    let mut left_input = BufReader::new(File::open(file1.clone())?);
    let mut right_input = BufReader::new(File::open(file2.clone())?);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    left_df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut writer,
        left_column,
        right_column,
        r#type,
    )?;
    print_info(&format!(
        "Successfully joined '{:?}' and '{:?}' on columns '{}' and '{}'",
        file1, file2, left_column, right_column
    ));
    Ok(())
}
