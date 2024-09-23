use prettytable::csv::{ReaderBuilder, Writer};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Instant;

/// Represents a data frame with CSV data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFrame {
    pub name: String,
    pub headers: Vec<String>,
    pub header_indices: HashMap<String, usize>,
    pub primary_key: Option<String>,
    pub foreign_keys: Vec<(String, String, String)>,
}

impl DataFrame {
    /// Creates a new DataFrame with the given name.
    pub fn new(name: String) -> Self {
        DataFrame {
            name,
            headers: Vec::new(),
            header_indices: HashMap::new(),
            primary_key: None,
            foreign_keys: vec![],
        }
    }

    /// Reads CSV headers from a file.
    pub fn read_csv_stream(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let file =
            File::open(path).map_err(|e| format!("Failed to open file '{:?}': {}", path, e))?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

        self.headers = reader.headers()?.iter().map(|s| s.to_string()).collect();
        self.header_indices = self
            .headers
            .iter()
            .enumerate()
            .map(|(i, h)| (h.clone(), i))
            .collect();

        Ok(())
    }

    /// Writes CSV headers to a writer.
    pub fn write_csv_stream<W: Write>(&self, writer: W) -> Result<(), Box<dyn Error>> {
        let mut csv_writer = Writer::from_writer(writer);
        csv_writer.write_record(&self.headers)?;
        Ok(())
    }

    /// Processes CSV rows with a custom function.
    pub fn process_rows<F>(
        &self,
        input: &mut dyn BufRead,
        mut processor: F,
    ) -> Result<(), Box<dyn Error>>
    where
        F: FnMut(&[String]) -> Result<(), Box<dyn Error>>,
    {
        let mut reader = ReaderBuilder::new().has_headers(false).from_reader(input);

        for result in reader.records() {
            let record = result?;
            let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
            processor(&row)?;
        }

        Ok(())
    }

    /// Concatenates CSV data.
    pub fn concat_stream<R: BufRead, W: Write>(
        &self,
        input: &mut R,
        output: &mut W,
    ) -> Result<(), Box<dyn Error>> {
        let timer = Instant::now();
        self.process_rows(input, |row| {
            writeln!(output, "{}", row.join(","))?;
            Ok(())
        })?;
        let duration = timer.elapsed();
        println!("Operation took: {:.2?}\n", duration);

        Ok(())
    }

    /// Drops specified columns from CSV data.
    pub fn drop_stream<R: BufRead, W: Write>(
        &self,
        input: &mut R,
        output: &mut W,
        columns: &[String],
    ) -> Result<(), Box<dyn Error>> {
        let indices_to_keep: Vec<usize> = self
            .headers
            .iter()
            .enumerate()
            .filter(|(_, h)| !columns.contains(h))
            .map(|(i, _)| i)
            .collect();

        let new_headers: Vec<String> = indices_to_keep
            .iter()
            .map(|&i| self.headers[i].clone())
            .collect();

        writeln!(output, "{}", new_headers.join(","))?;

        let timer = Instant::now();
        self.process_rows(input, |row| {
            let new_row: Vec<String> = indices_to_keep.iter().map(|&i| row[i].clone()).collect();
            writeln!(output, "{}", new_row.join(","))?;
            Ok(())
        })?;
        let duration = timer.elapsed();
        println!("Operation took: {:.2?}\n", duration);

        Ok(())
    }

    /// Selects specified columns from CSV data.
    pub fn select_stream<R: BufRead, W: Write>(
        &self,
        input: &mut R,
        output: &mut W,
        columns: &[String],
    ) -> Result<(), Box<dyn Error>> {
        let columns_to_drop: Vec<String> = self
            .headers
            .iter()
            .filter(|h| !columns.contains(h))
            .cloned()
            .collect();

        let timer = Instant::now();
        self.drop_stream(input, output, &columns_to_drop)?;
        let duration = timer.elapsed();
        println!("Operation took: {:.2?}\n", duration);

        Ok(())
    }

    /// Performs a join operation on two CSV streams.
    pub fn join_stream<R1: BufRead, R2: BufRead, W: Write>(
        &self,
        left_input: &mut R1,
        right_input: &mut R2,
        output: &mut W,
        left_key: &str,
        right_key: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Function to parse CSV line
        let timer = Instant::now();
        fn parse_csv_line(line: &str) -> Vec<String> {
            line.split(',').map(|s| s.trim().to_string()).collect()
        }

        let left_index = self
            .headers
            .iter()
            .position(|h| h == left_key)
            .ok_or_else(|| format!("Column '{}' not found in left table", left_key))?;

        let mut right_reader = BufReader::new(right_input);
        let mut right_headers_line = String::new();
        right_reader.read_line(&mut right_headers_line)?;
        let right_headers = parse_csv_line(&right_headers_line);

        let right_index = right_headers
            .iter()
            .position(|h| h == right_key)
            .ok_or_else(|| format!("Column '{}' not found in right table", right_key))?;

        let mut joined_headers = self.headers.clone();
        joined_headers.extend(right_headers.iter().filter(|&h| h != right_key).cloned());
        writeln!(output, "{}", joined_headers.join(","))?;

        let mut right_index_map: BTreeMap<String, Vec<Vec<String>>> = BTreeMap::new();
        for line in right_reader.lines() {
            let record = parse_csv_line(&line?);
            if record.len() > right_index {
                let key = record[right_index].to_string();
                right_index_map.entry(key).or_default().push(record);
            }
        }

        let left_reader = BufReader::new(left_input);
        let mut join_count = 0;
        let mut last_key = String::new();

        for line in left_reader.lines() {
            let left_record = parse_csv_line(&line?);
            if left_record.len() > left_index {
                let left_key_value = left_record[left_index].to_string();

                if left_key_value != last_key {
                    join_count = 0;
                    last_key = left_key_value.clone();
                }

                if let Some(right_rows) = right_index_map.get(&left_key_value) {
                    if join_count < right_rows.len() {
                        let right_row = &right_rows[join_count];
                        let mut joined_row = left_record.clone();
                        joined_row.extend(
                            right_row
                                .iter()
                                .enumerate()
                                .filter(|&(i, _)| i != right_index)
                                .map(|(_, v)| v.clone()),
                        );
                        writeln!(output, "{}", joined_row.join(","))?;
                        join_count += 1;
                    }
                }
            }
        }

        let duration = timer.elapsed();
        println!("Operation took: {:.2?}\n", duration);
        Ok(())
    }
}

/// Reads and prints CSV data with optional line count and reverse order.
pub fn read_csv_stream(
    path: &Path,
    lines_count: Option<usize>,
    reverse: bool,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;

    if reverse {
        lines.reverse();
    }

    let count = lines_count.unwrap_or(lines.len());
    for line in lines.into_iter().take(count) {
        println!("{}", line);
    }

    Ok(())
}

pub fn human_readable_bytes(bytes: u64) -> String {
    let sizes = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut i = 0;

    while size >= 1024.0 && i < sizes.len() - 1 {
        size /= 1024.0;
        i += 1;
    }

    format!("{:.2} {}", size, sizes[i])
}
