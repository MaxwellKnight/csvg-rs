//! Functions for creating a graph from tables, running the `dot` command, and opening files.
use crate::{config, csv::DataFrame, sql};
use petgraph::graph::UnGraph;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, path::PathBuf, process::Command};

#[derive(Serialize, Deserialize)]
pub struct SerializableGraph {
    pub nodes: Vec<DataFrame>,
    pub edges: Vec<(usize, usize, (String, String))>,
}

impl SerializableGraph {
    pub fn into_graph(self) -> UnGraph<DataFrame, (String, String)> {
        let mut graph = UnGraph::new_undirected();
        let mut node_map = HashMap::new();

        // Add nodes
        for (index, node) in self.nodes.into_iter().enumerate() {
            let node_index = graph.add_node(node);
            node_map.insert(index, node_index);
        }

        // Add edges
        for (source, target, weight) in self.edges {
            graph.add_edge(node_map[&source], node_map[&target], weight);
        }

        graph
    }
}

impl From<&UnGraph<DataFrame, (String, String)>> for SerializableGraph {
    fn from(graph: &UnGraph<DataFrame, (String, String)>) -> Self {
        let nodes: Vec<DataFrame> = graph.node_weights().cloned().collect();
        let edges: Vec<(usize, usize, (String, String))> = graph
            .edge_indices()
            .map(|edge_index| {
                let (source, target) = graph.edge_endpoints(edge_index).unwrap();
                let weight = graph.edge_weight(edge_index).unwrap().clone();
                (source.index(), target.index(), weight)
            })
            .collect();

        SerializableGraph { nodes, edges }
    }
}

/// Creates an undirected graph from a vector of `DataFrame` instances.
pub fn create_graph(nodes: Vec<DataFrame>) -> UnGraph<DataFrame, (String, String)> {
    let mut g = UnGraph::<DataFrame, (String, String)>::new_undirected();

    // Add nodes to the graph
    for node in nodes {
        g.add_node(node);
    }

    // Add edges based on foreign key relationships
    for src_index in g.node_indices() {
        let src_table = &g[src_index];

        for (src_column, dst_table_name, dst_column) in src_table.foreign_keys.clone() {
            if let Some((dst_index, _)) = g
                .node_indices()
                .map(|idx| (idx, &g[idx]))
                .find(|(_, table)| table.name == *dst_table_name)
            {
                g.add_edge(src_index, dst_index, (src_column, dst_column));
            }
        }
    }

    g
}

pub fn generate_graph(
    config_dir: &PathBuf,
) -> Result<UnGraph<DataFrame, (String, String)>, Box<dyn Error>> {
    let schema_path =
        config::find_sql_schema().ok_or("No SQL schema found in the current directory")?;
    let schema_content = std::fs::read_to_string(&schema_path)?;
    let result = sql::parse_sql(&schema_content)?;
    let g = create_graph(result);
    config::write_graph_cache(&g, config_dir)?;
    Ok(g)
}

/// Opens a file using the default application based on the operating system.
pub fn open_dot_file(file_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(&["/C", "start", file_path.to_str().unwrap()])
            .status()?;
    } else if cfg!(target_os = "macos") {
        Command::new("open").arg(file_path).status()?;
    } else if cfg!(target_os = "linux") {
        Command::new("xdg-open").arg(file_path).status()?;
    } else {
        println!("Unsupported platform: unable to open the file automatically.");
    }

    Ok(())
}

/// Generates DOT format content for an undirected graph.
pub fn write_dot_file(g: &UnGraph<DataFrame, (String, String)>) -> String {
    let dot_content = {
        let mut dot = String::new();
        dot.push_str("graph G {\n");
        dot.push_str("  node [shape=record, fontname=\"Arial\"];\n");
        dot.push_str("  edge [fontsize=12];\n");
        dot.push_str("  nodesep=1.0;\n");
        dot.push_str("  edgesep=0.75;\n");
        dot.push_str("  rankdir=TB;\n");
        for node in g.node_indices() {
            let table = &g[node];
            let columns = table
                .headers
                .iter()
                .map(|col| col.clone())
                .collect::<Vec<_>>()
                .join("|");
            dot.push_str(&format!(
                "  {} [label=<{{<b><font point-size='16' color='red'>{}</font></b>|{}}}>];\n",
                node.index(),
                table.name,
                columns
            ));
        }
        for edge in g.edge_indices() {
            let (src, dst) = g.edge_endpoints(edge).unwrap();
            let (label1, label2) = g.edge_weight(edge).unwrap();
            dot.push_str(&format!(
                "  {} -- {} [label=\"({}, {})\"];\n",
                src.index(),
                dst.index(),
                label1,
                label2
            ));
        }
        dot.push_str("}\n");
        dot
    };
    dot_content
}
