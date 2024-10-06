use std::{
    error::Error,
    io::{BufWriter, Cursor, Write},
};
use tempfile::NamedTempFile;

use csvg::{
    cli::JoinType,
    csv::{human_readable_bytes, DataFrame},
};

#[test]
fn test_dataframe_new() {
    let df = DataFrame::new("test".to_string());
    assert_eq!(df.name, "test");
    assert!(df.headers.is_empty());
    assert!(df.header_indices.is_empty());
    assert!(df.primary_key.is_none());
    assert!(df.foreign_keys.is_empty());
}

#[test]
fn test_read_csv_stream() -> Result<(), Box<dyn Error>> {
    let mut file = NamedTempFile::new()?;
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        writer.write_all(b"header1,header2,header3\n")?;
    }

    let mut df = DataFrame::new("test".to_string());
    df.read_headers(file.path())?;

    assert_eq!(df.headers, vec!["header1", "header2", "header3"]);
    assert_eq!(df.header_indices.len(), 3);
    assert_eq!(df.header_indices["header1"], 0);
    assert_eq!(df.header_indices["header2"], 1);
    assert_eq!(df.header_indices["header3"], 2);

    Ok(())
}

#[test]
fn test_write_csv_stream() -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new("test".to_string());
    df.headers = vec![
        "header1".to_string(),
        "header2".to_string(),
        "header3".to_string(),
    ];

    let mut output = Vec::new();
    df.write_headers(&mut output)?;

    assert_eq!(String::from_utf8(output)?, "header1,header2,header3\n");
    Ok(())
}

#[test]
fn test_process_rows() -> Result<(), Box<dyn Error>> {
    let df = DataFrame::new("test".to_string());
    let input = Cursor::new("value1,value2,value3\nvalue4,value5,value6");
    let mut result = Vec::new();

    df.process_rows(&mut input.clone(), |row| {
        result.push(row.to_vec());
        Ok(())
    })?;

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], vec!["value1", "value2", "value3"]);
    assert_eq!(result[1], vec!["value4", "value5", "value6"]);
    Ok(())
}

#[test]
fn test_concat_stream() -> Result<(), Box<dyn Error>> {
    let df = DataFrame::new("test".to_string());
    let mut input = Cursor::new("value1,value2,value3\nvalue4,value5,value6");
    let mut output = Vec::new();

    df.concat_stream(&mut input, &mut output)?;

    assert_eq!(
        String::from_utf8(output)?,
        "value1,value2,value3\nvalue4,value5,value6\n"
    );
    Ok(())
}

#[test]
fn test_drop_stream() -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new("test".to_string());
    df.headers = vec![
        "header1".to_string(),
        "header2".to_string(),
        "header3".to_string(),
    ];
    let mut input = Cursor::new("value1,value2,value3\nvalue4,value5,value6");
    let mut output = Vec::new();

    df.drop_stream(&mut input, &mut output, &["header2".to_string()])?;

    assert_eq!(
        String::from_utf8(output)?,
        "header1,header3\nvalue1,value3\nvalue4,value6\n"
    );
    Ok(())
}

#[test]
fn test_select_stream() -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new("test".to_string());
    df.headers = vec![
        "header1".to_string(),
        "header2".to_string(),
        "header3".to_string(),
    ];
    let mut input = Cursor::new("value1,value2,value3\nvalue4,value5,value6");
    let mut output = Vec::new();

    df.select_stream(
        &mut input,
        &mut output,
        &["header1".to_string(), "header3".to_string()],
    )?;

    assert_eq!(
        String::from_utf8(output)?,
        "header1,header3\nvalue1,value3\nvalue4,value6\n"
    );
    Ok(())
}

#[test]
fn test_join_stream() -> Result<(), Box<dyn Error>> {
    let mut df = DataFrame::new("test".to_string());
    df.headers = vec!["id".to_string(), "name".to_string()];
    let mut left_input = Cursor::new("id,name\n1,Alice\n2,Bob");
    let mut right_input = Cursor::new("id,age\n1,30\n2,25");
    let mut output = Vec::new();

    df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut output,
        "id",
        "id",
        &JoinType::Inner,
    )?;

    assert_eq!(
        String::from_utf8(output)?,
        "id,name,age\n1,Alice,30\n2,Bob,25\n"
    );
    Ok(())
}

fn setup_dataframe() -> DataFrame {
    let mut df = DataFrame::new("test".to_string());
    df.headers = vec!["id".to_string(), "name".to_string()];
    df
}

#[test]
fn test_inner_join() -> Result<(), Box<dyn Error>> {
    let df = setup_dataframe();
    let mut left_input = Cursor::new("id,name\n1,Alice\n2,Bob\n3,Charlie");
    let mut right_input = Cursor::new("id,age\n1,30\n2,25\n4,35");
    let mut output = Vec::new();
    df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut output,
        "id",
        "id",
        &JoinType::Inner,
    )?;
    assert_eq!(
        String::from_utf8(output)?,
        "id,name,age\n1,Alice,30\n2,Bob,25\n"
    );
    Ok(())
}

#[test]
fn test_left_outer_join() -> Result<(), Box<dyn Error>> {
    let df = setup_dataframe();
    let mut left_input = Cursor::new("id,name\n1,Alice\n2,Bob\n3,Charlie");
    let mut right_input = Cursor::new("id,age\n1,30\n2,25\n4,35");
    let mut output = Vec::new();
    df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut output,
        "id",
        "id",
        &JoinType::Left,
    )?;
    assert_eq!(
        String::from_utf8(output)?,
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,\n"
    );
    Ok(())
}

#[test]
fn test_right_outer_join() -> Result<(), Box<dyn Error>> {
    let df = setup_dataframe();
    let mut left_input = Cursor::new("id,name\n1,Alice\n2,Bob\n3,Charlie");
    let mut right_input = Cursor::new("id,age\n1,30\n2,25\n4,35");
    let mut output = Vec::new();
    df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut output,
        "id",
        "id",
        &JoinType::Right,
    )?;
    assert_eq!(
        String::from_utf8(output)?,
        "id,name,age\n1,Alice,30\n2,Bob,25\n,,35\n"
    );
    Ok(())
}

#[test]
fn test_full_outer_join() -> Result<(), Box<dyn Error>> {
    let df = setup_dataframe();
    let mut left_input = Cursor::new("id,name\n1,Alice\n2,Bob\n3,Charlie");
    let mut right_input = Cursor::new("id,age\n1,30\n2,25\n4,35");
    let mut output = Vec::new();
    df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut output,
        "id",
        "id",
        &JoinType::Full,
    )?;
    assert_eq!(
        String::from_utf8(output)?,
        "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,\n,,35\n"
    );
    Ok(())
}

#[test]
fn test_join_with_multiple_matches() -> Result<(), Box<dyn Error>> {
    let df = setup_dataframe();
    let mut left_input = Cursor::new("id,name\n1,Alice\n2,Bob\n2,Charlie");
    let mut right_input = Cursor::new("id,age\n1,30\n2,25\n2,35");
    let mut output = Vec::new();
    df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut output,
        "id",
        "id",
        &JoinType::Inner,
    )?;
    assert_eq!(
        String::from_utf8(output)?,
        "id,name,age\n1,Alice,30\n2,Bob,25\n2,Bob,35\n2,Charlie,25\n2,Charlie,35\n"
    );
    Ok(())
}

#[test]
fn test_join_with_empty_inputs() -> Result<(), Box<dyn Error>> {
    let df = setup_dataframe();
    let mut left_input = Cursor::new("id,name\n");
    let mut right_input = Cursor::new("id,age\n");
    let mut output = Vec::new();
    df.join_stream(
        &mut left_input,
        &mut right_input,
        &mut output,
        "id",
        "id",
        &JoinType::Full,
    )?;
    assert_eq!(String::from_utf8(output)?, "id,name,age\n");
    Ok(())
}

#[test]
fn test_human_readable_bytes() {
    assert_eq!(human_readable_bytes(500), "500.00 B");
    assert_eq!(human_readable_bytes(1024), "1.00 KB");
    assert_eq!(human_readable_bytes(1_048_576), "1.00 MB");
    assert_eq!(human_readable_bytes(1_073_741_824), "1.00 GB");
    assert_eq!(human_readable_bytes(1_099_511_627_776), "1.00 TB");
}
