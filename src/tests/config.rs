use petgraph::graph::UnGraph;
use std::path::PathBuf;
use tempfile::TempDir;

use crate::config::{
    create_config_folder, graph_cache_exists, read_config, read_graph_cache, redirect_output,
    write_config, write_graph_cache, Config, GraphvizSettings,
};
use crate::csv::DataFrame;

#[test]
fn test_write_and_read_config() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.json");

    let config = Config {
        output_file: "test_output".to_string(),
        output_path: PathBuf::from("/test/output"),
        source_path: PathBuf::from("/test/source"),
        graphviz_settings: GraphvizSettings {
            engine: "neato".to_string(),
            format: "svg".to_string(),
        },
        csv_output_path: PathBuf::from("/test/csv"),
    };

    write_config(&config, &config_path).unwrap();
    let read_config = read_config(&temp_dir.path()).unwrap();

    assert_eq!(config.output_file, read_config.output_file);
    assert_eq!(config.output_path, read_config.output_path);
    assert_eq!(config.source_path, read_config.source_path);
    assert_eq!(
        config.graphviz_settings.engine,
        read_config.graphviz_settings.engine
    );
    assert_eq!(
        config.graphviz_settings.format,
        read_config.graphviz_settings.format
    );
    assert_eq!(config.csv_output_path, read_config.csv_output_path);
}

#[test]
fn test_write_and_read_graph_cache() {
    let temp_dir = TempDir::new().unwrap();
    let mut graph = UnGraph::new_undirected();
    let node1 = graph.add_node(DataFrame::new("Table1".to_string()));
    let node2 = graph.add_node(DataFrame::new("Table2".to_string()));
    graph.add_edge(node1, node2, ("col1".to_string(), "col2".to_string()));

    write_graph_cache(&graph, temp_dir.path()).unwrap();
    assert!(graph_cache_exists(temp_dir.path()));

    let read_graph = read_graph_cache(temp_dir.path()).unwrap();
    assert_eq!(read_graph.node_count(), graph.node_count());
    assert_eq!(read_graph.edge_count(), graph.edge_count());
}

#[test]
fn test_redirect_output() {
    let temp_dir = TempDir::new().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    create_config_folder().unwrap();

    redirect_output(Some("new_output.txt".to_string())).unwrap();

    let config = read_config(&temp_dir.path().join(".csvgraph")).unwrap();
    assert_eq!(
        config.output_file, "new_output.txt",
        "Output file was not updated in config"
    );

    // Print the contents of the config file for debugging
    println!("Contents of config file:");
    let config_contents =
        std::fs::read_to_string(temp_dir.path().join(".csvgraph").join("config.json")).unwrap();
    println!("{}", config_contents);
}
