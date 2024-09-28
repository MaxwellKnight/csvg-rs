use petgraph::graph::UnGraph;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::{env, fs, io};

use crate::csv::DataFrame;
use crate::graph::SerializableGraph;
use crate::utils;

/// Configuration settings for output paths and Graphviz.
#[derive(Serialize, Deserialize)]
pub struct Config {
    pub output_file: String,
    pub output_path: PathBuf, // Path for generated files
    pub source_path: PathBuf,
    pub graphviz_settings: GraphvizSettings, // Graphviz rendering settings
    pub csv_output_path: PathBuf,            // Path for CSV files
}

/// Graphviz rendering settings.
#[derive(Serialize, Deserialize)]
pub struct GraphvizSettings {
    pub engine: String, // Engine to use (e.g., "dot")
    pub format: String, // Output format (e.g., "png")
}

impl Default for Config {
    fn default() -> Self {
        Config {
            output_file: String::from("output.csv"),
            output_path: PathBuf::from(".csvgraph/generated-files"),
            source_path: PathBuf::from("./"),
            graphviz_settings: GraphvizSettings {
                engine: "dot".to_string(),
                format: "png".to_string(),
            },
            csv_output_path: PathBuf::from("csv"),
        }
    }
}

/// Creates configuration folder and file if missing.
pub fn create_config_folder() -> Result<PathBuf, io::Error> {
    let current_dir = std::env::current_dir().map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to get current directory: {}", e),
        )
    })?;
    let config_dir = current_dir.join(".csvgraph");
    fs::create_dir_all(&config_dir).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to create config directory {:?}: {}", config_dir, e),
        )
    })?;

    let cfg = Config::default();
    let config_file = config_dir.join("config.json");

    if !config_file.exists() {
        write_config(&cfg, &config_file).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to write initial config file: {}", e),
            )
        })?;
    }

    Ok(config_dir)
}

/// Writes configuration to a JSON file.
pub fn write_config(config: &Config, config_path: &Path) -> io::Result<()> {
    let config_json = serde_json::to_string_pretty(config).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to serialize config: {}", e),
        )
    })?;
    fs::write(config_path, config_json)
}

/// Reads configuration from a JSON file.
pub fn read_config(config_dir: &Path) -> std::io::Result<Config> {
    let config_path = config_dir.join("config.json");
    if config_path.exists() {
        let config_json = fs::read_to_string(config_path)?;
        let config: Config = serde_json::from_str(&config_json)?;
        return Ok(config);
    }
    Ok(Config::default())
}

/// Finds the first `.sql` file in the current directory.
pub fn find_sql_schema() -> Option<PathBuf> {
    fs::read_dir(".")
        .ok()?
        .filter_map(Result::ok)
        .find(|entry| entry.path().extension().map_or(false, |ext| ext == "sql"))
        .map(|entry| entry.path())
}

/// Serializes and caches the graph to a file.
pub fn write_graph_cache(
    graph: &UnGraph<DataFrame, (String, String)>,
    config_dir: &Path,
) -> io::Result<()> {
    let graph_path = config_dir.join("graph.json");
    let serializable = SerializableGraph::from(graph);
    let serialized = serde_json::to_string(&serializable).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to serialize graph: {}", e),
        )
    })?;
    fs::write(graph_path, serialized)?;
    Ok(())
}

/// Reads the cached graph from a file.
pub fn read_graph_cache(config_dir: &Path) -> io::Result<UnGraph<DataFrame, (String, String)>> {
    let graph_path = config_dir.join("graph.json");
    let serialized = fs::read_to_string(graph_path)?;
    let serializable: SerializableGraph = serde_json::from_str(&serialized).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to deserialize graph: {}", e),
        )
    })?;
    Ok(serializable.into_graph())
}

/// Checks if the graph cache file exists.
pub fn graph_cache_exists(config_dir: &Path) -> bool {
    config_dir.join("graph.json").exists()
}

pub fn redirect_output(output: Option<String>) -> Result<(), Box<dyn Error>> {
    if let Some(output) = output {
        let config_dir = create_config_folder().map_err(|e| {
            eprintln!("Failed to create config folder: {}", e);
            e
        })?;

        let mut config = read_config(&config_dir).map_err(|e| {
            eprintln!("Failed to read config from {:?}: {}", config_dir, e);
            e
        })?;

        config.output_file = output.clone();

        let config_path = config_dir.join("config.json");
        write_config(&config, &config_path).map_err(|e| {
            eprintln!("Failed to write config to {:?}: {}", config_path, e);
            e
        })?;

        utils::print_info(&format!("Output file updated successfully to: {}", output));
    }

    Ok(())
}

pub fn display_relative_path(path: &Path) -> String {
    match env::current_dir() {
        Ok(current_dir) => path
            .strip_prefix(&current_dir)
            .unwrap_or(path)
            .display()
            .to_string(),
        Err(_) => path.display().to_string(),
    }
}
