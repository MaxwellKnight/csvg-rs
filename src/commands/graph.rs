use crate::cli::{DisplayType, GraphArgs, GraphSubcommands, JoinType};
use crate::config::{self, Config};
use crate::csv::{human_readable_bytes, DataFrame};
use crate::graph;
use crate::utils::print_info;
use petgraph::algo::dijkstra;
use petgraph::data::FromElements;
use petgraph::graph::{NodeIndex, UnGraph};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;

/// Execute graph operations based on command line arguments.
pub fn execute(args: &GraphArgs) -> Result<(), Box<dyn Error>> {
    let config_dir = config::create_config_folder()?;
    let config: Config = config::read_config(&config_dir)?;

    if args.regenerate || !config::graph_cache_exists(&config_dir) {
        regenerate_graph_cache(&config_dir)?;
        return Ok(());
    }

    let g = config::read_graph_cache(&config_dir)?;

    match &args.subcommand {
        Some(subcommand) => match subcommand {
            GraphSubcommands::Create { schema, format } => {
                handle_graph_create(schema, &config, &g, get_type(format))
            }
            GraphSubcommands::ShortestPath { from, to } => handle_graph_shortest_path(from, to, &g),
            GraphSubcommands::Join {
                left_table,
                right_table,
                ..
            } => handle_graph_join(&config, left_table, right_table, &g),
            GraphSubcommands::Mst => handle_graph_mst(&g, &config),
            GraphSubcommands::Display { format } => {
                handle_graph_display(&g, &config, "graph", get_type(format))
            }
        },
        None => Ok(()),
    }
}

fn get_type(format: &DisplayType) -> &str {
    match format {
        DisplayType::Pdf => "pdf",
        DisplayType::Png => "png",
    }
}

/// Regenerate and cache the graph data.
pub fn regenerate_graph_cache(config_dir: &Path) -> Result<(), Box<dyn Error>> {
    print_info("Generating new graph data.");
    let g = graph::generate_graph(&config_dir.to_path_buf())?;
    config::write_graph_cache(&g, config_dir)?;
    print_info("Graph data regenerated and cached.");
    Ok(())
}

/// Handle the creation of a graph based on a schema.
fn handle_graph_create(
    schema: &str,
    config: &Config,
    g: &UnGraph<DataFrame, (String, String)>,
    format: &str,
) -> Result<(), Box<dyn Error>> {
    let _schema_path = if !schema.is_empty() {
        Path::new(schema).to_path_buf()
    } else {
        config::find_sql_schema().ok_or("No SQL schema found in the current directory")?
    };

    let dot_content = graph::write_dot_file(g);
    let output_dir = Path::new(&config.output_path);
    std::fs::create_dir_all(output_dir)?;

    let dot_file = output_dir.join("graph.dot");
    let png_file = output_dir.join(format!("graph.{}", format));

    save_dot_file(&dot_file, &dot_content)?;
    run_dot_command(&dot_file, &png_file, format)?;
    graph::open_dot_file(&png_file)?;

    Ok(())
}

/// Handle the join operation between two tables in the graph.
fn handle_graph_join(
    config: &Config,
    left_table: &str,
    right_table: &str,
    g: &UnGraph<DataFrame, (String, String)>,
) -> Result<(), Box<dyn Error>> {
    let left_node = find_node(g, left_table)?;
    let right_node = find_node(g, right_table)?;

    let path = find_shortest_path(g, left_node, right_node)?;
    join_tables_along_path(g, &path, config)?;

    print_info("Join operation completed successfully.");
    Ok(())
}

/// Find a node in the graph by table name.
pub fn find_node(
    g: &UnGraph<DataFrame, (String, String)>,
    table: &str,
) -> Result<NodeIndex, Box<dyn Error>> {
    g.node_indices()
        .find(|&node| g[node].name == table)
        .ok_or_else(|| format!("Table '{}' not found in graph", table).into())
}

/// Find the shortest path between two nodes in the graph.
pub fn find_shortest_path(
    g: &UnGraph<DataFrame, (String, String)>,
    start: NodeIndex,
    end: NodeIndex,
) -> Result<Vec<NodeIndex>, Box<dyn Error>> {
    let res = dijkstra(g, start, Some(end), |_| 1);
    let mut path = Vec::new();
    let mut current = end;

    while current != start {
        path.push(current);
        current = g
            .neighbors(current)
            .filter(|n| res.contains_key(n))
            .min_by_key(|n| res[n])
            .ok_or("Path reconstruction failed")?;
    }
    path.push(start);
    path.reverse();
    Ok(path)
}

/// Join tables along the shortest path between two nodes.
fn join_tables_along_path(
    g: &UnGraph<DataFrame, (String, String)>,
    path: &[NodeIndex],
    config: &Config,
) -> Result<(), Box<dyn Error>> {
    if path.is_empty() {
        return Err("Path is empty".into());
    }

    let mut current_df = g[path[0]].clone();
    let mut temp_file = NamedTempFile::new()?;

    // Copy first table to temp file
    {
        let mut writer = BufWriter::new(&temp_file);
        let mut reader = BufReader::new(File::open(
            &config.source_path.join(format!("{}.csv", current_df.name)),
        )?);
        let bytes_copied = std::io::copy(&mut reader, &mut writer)?;
        writer.flush()?;
        print_info(&format!(
            "Initial file size: {}",
            human_readable_bytes(bytes_copied)
        ));
    }

    for (i, (_, &next_node)) in path.iter().zip(path.iter().skip(1)).enumerate() {
        let next_df = &g[next_node];
        print_info(&format!("Joining {} and {}", current_df.name, next_df.name));

        let mut left_reader = BufReader::new(temp_file.reopen()?);
        let mut right_reader = BufReader::new(File::open(
            &config.source_path.join(format!("{}.csv", next_df.name)),
        )?);

        let new_temp_file = NamedTempFile::new()?;
        let (left_col, right_col) = find_join_columns(&current_df, next_df)?;
        {
            let mut writer = BufWriter::new(&new_temp_file);

            current_df.join_stream(
                &mut left_reader,
                &mut right_reader,
                &mut writer,
                &left_col,
                &right_col,
                &JoinType::Inner,
            )?;
            writer.flush()?;
        }

        temp_file = new_temp_file;
        let file_size = temp_file.as_file().metadata()?.len();
        print_info(&format!(
            "Size after join {}: {}",
            i + 1,
            human_readable_bytes(file_size)
        ));
        if file_size == 0 {
            print_info("Warning: Join produced no results");
            return Err("Join produced no results".into());
        }

        current_df = update_dataframe_after_join(&current_df, next_df, &left_col, &right_col);
    }

    if let Some(parent) = Path::new(&config.output_file).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut final_reader = BufReader::new(temp_file.reopen()?);
    let stdout = std::io::stdout();
    let mut final_writer = BufWriter::new(stdout.lock());
    let bytes_copied = std::io::copy(&mut final_reader, &mut final_writer)?;
    final_writer.flush()?;

    print_info(&format!(
        "written {} to {}",
        human_readable_bytes(bytes_copied),
        config.output_file
    ));

    Ok(())
}

/// Find suitable join columns between two DataFrames.
pub fn find_join_columns(
    left: &DataFrame,
    right: &DataFrame,
) -> Result<(String, String), Box<dyn Error>> {
    for (left_col, _, right_col) in &left.foreign_keys {
        if right.headers.contains(right_col) {
            return Ok((left_col.clone(), right_col.clone()));
        }
    }
    for (right_col, _, left_col) in &right.foreign_keys {
        if left.headers.contains(left_col) {
            return Ok((left_col.clone(), right_col.clone()));
        }
    }
    Err("No suitable join columns found".into())
}

/// Update the DataFrame after a join operation.
pub fn update_dataframe_after_join(
    left_df: &DataFrame,
    right_df: &DataFrame,
    left_col: &str,
    right_col: &str,
) -> DataFrame {
    let mut new_df = left_df.clone();

    new_df
        .headers
        .extend(right_df.headers.iter().filter(|&h| h != right_col).cloned());

    new_df.header_indices = new_df
        .headers
        .iter()
        .enumerate()
        .map(|(i, h)| (h.clone(), i))
        .collect();

    new_df.foreign_keys.extend(
        right_df
            .foreign_keys
            .iter()
            .filter(|&(col, _, _)| col != right_col)
            .cloned(),
    );

    if right_df.primary_key.as_ref() == Some(&right_col.to_string()) {
        new_df.primary_key = Some(left_col.to_string());
    }

    new_df
}

/// Handle the shortest path operation between two nodes in the graph.
fn handle_graph_shortest_path(
    from: &str,
    to: &str,
    g: &UnGraph<DataFrame, (String, String)>,
) -> Result<(), Box<dyn Error>> {
    let from_index = find_node(g, from)?;
    let to_index = find_node(g, to)?;

    let path = find_shortest_path(g, from_index, to_index)?;
    let path_str: Vec<String> = path.iter().map(|&n| g[n].name.clone()).collect();
    println!("Shortest path: {}", path_str.join(" -> "));

    Ok(())
}

/// Handle the Minimum Spanning Tree operation .
fn handle_graph_mst(
    g: &UnGraph<DataFrame, (String, String)>,
    config: &Config,
) -> Result<(), Box<dyn Error>> {
    let mst = petgraph::algo::min_spanning_tree(g);
    let mst: UnGraph<DataFrame, (String, String)> = petgraph::Graph::from_elements(mst);
    handle_graph_display(&mst, &config, "mst", "png")
}
/// Handle the display of the graph.
fn handle_graph_display(
    g: &UnGraph<DataFrame, (String, String)>,
    config: &Config,
    output: &str,
    format: &str,
) -> Result<(), Box<dyn Error>> {
    let dot_content = graph::write_dot_file(g);
    let output_dir = Path::new(&config.output_path);
    std::fs::create_dir_all(output_dir)?;

    let dot_file = output_dir.join(format!("{}.dot", output));
    let png_file = output_dir.join(format!("{}.{}", output, format));

    save_dot_file(&dot_file, &dot_content)?;
    run_dot_command(&dot_file, &png_file, format)?;
    graph::open_dot_file(&png_file)?;

    Ok(())
}

/// Save the DOT file content to a file.
fn save_dot_file(dot_file: &Path, content: &str) -> Result<(), Box<dyn Error>> {
    let mut file = File::create(dot_file)?;
    file.write_all(content.as_bytes())?;
    print_info(&format!("DOT file saved to {}", dot_file.display()));
    Ok(())
}

/// Run the 'dot' command to generate a PNG file from the DOT file.
fn run_dot_command(
    dot_file: &Path,
    output_file: &Path,
    format: &str,
) -> Result<(), Box<dyn Error>> {
    let mut cmd = Command::new("dot")
        .args(&[
            &format!("-T{}", format),
            dot_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let status = cmd.wait()?;

    if status.success() {
        print_info(&format!(
            "{} file saved to {}",
            format.to_uppercase(),
            output_file.display()
        ));
        Ok(())
    } else {
        Err(format!("Failed to run `dot` command: {:?}", status).into())
    }
}
