use crate::cli::{CsvArgs, CsvSubcommands};
use crate::csv::{self, DataFrame};
use crate::utils::print_info;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read};
use std::path::Path;

/// Execute CSV operations based on command line arguments.
pub fn execute(args: &CsvArgs) -> Result<(), Box<dyn Error>> {
    match &args.subcommand {
        CsvSubcommands::Head { file, lines } => handle_head(file, *lines),
        CsvSubcommands::Tail { file, lines } => handle_tail(file, *lines),
        CsvSubcommands::Concat { files } => handle_concat(files),
        CsvSubcommands::Drop { file, columns } => handle_drop(file, columns),
        CsvSubcommands::Select { file, columns } => handle_select(file, columns),
        CsvSubcommands::Join {
            file1,
            file2,
            left_column,
            right_column,
            ..
        } => handle_join(file1, file2, left_column, right_column),
    }
}

/// Display the first n lines of a CSV file.
fn handle_head(file: &str, lines: usize) -> Result<(), Box<dyn Error>> {
    let file_path = Path::new(file);
    csv::read_csv_stream(file_path, Some(lines), false)?;
    print_info(&format!(
        "Successfully displayed first {} lines from '{}'",
        lines, file
    ));
    Ok(())
}

/// Display the last n lines of a CSV file.
fn handle_tail(file: &str, lines: usize) -> Result<(), Box<dyn Error>> {
    let file_path = Path::new(file);
    csv::read_csv_stream(file_path, Some(lines), true)?;
    print_info(&format!(
        "Successfully displayed last {} lines from '{}'",
        lines, file
    ));
    Ok(())
}

/// Concatenate multiple CSV files.
fn handle_concat(files: &[String]) -> Result<(), Box<dyn Error>> {
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
        let mut input = BufReader::new(File::open(file)?);
        df.concat_stream(&mut input.by_ref(), &mut writer)?;
    }
    print_info(&format!("Successfully concatenated {} files", files.len()));
    Ok(())
}

/// Drop specified columns from a CSV file.
fn handle_drop(file: &str, columns: &[String]) -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new(file.to_string());
    df.read_csv_stream(Path::new(file))?;

    let mut input = BufReader::new(File::open(file)?);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    df.drop_stream(&mut input.by_ref(), &mut writer, columns)?;
    print_info(&format!(
        "Successfully dropped columns {:?} from '{}'",
        columns, file
    ));
    Ok(())
}

/// Select specified columns from a CSV file.
fn handle_select(file: &str, columns: &[String]) -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new(file.to_string());
    df.read_csv_stream(Path::new(file))?;

    let mut input = BufReader::new(File::open(file)?);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    df.select_stream(&mut input.by_ref(), &mut writer, columns)?;
    print_info(&format!(
        "Successfully selected columns {:?} from '{}'",
        columns, file
    ));
    Ok(())
}

/// Join two CSV files based on specified columns.
fn handle_join(
    file1: &str,
    file2: &str,
    left_column: &str,
    right_column: &str,
) -> Result<(), Box<dyn Error>> {
    let mut left_df = DataFrame::new(file1.to_string());
    left_df.read_csv_stream(Path::new(file1))?;

    let mut left_input = BufReader::new(File::open(file1)?);
    let mut right_input = BufReader::new(File::open(file2)?);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    left_df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut writer,
        left_column,
        right_column,
    )?;
    print_info(&format!(
        "Successfully joined '{}' and '{}' on columns '{}' and '{}'",
        file1, file2, left_column, right_column
    ));
    Ok(())
}
