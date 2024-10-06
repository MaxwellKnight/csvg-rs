use petgraph::graph::UnGraph;

use csvg::graph::{self, SerializableGraph};
use std::collections::HashSet;

use csvg::csv::DataFrame;

// Helper function to create a sample DataFrame
fn create_sample_dataframe(
    name: &str,
    headers: Vec<&str>,
    foreign_keys: Vec<(&str, &str, &str)>,
) -> DataFrame {
    DataFrame {
        name: name.to_string(),
        headers: headers.clone().into_iter().map(String::from).collect(),
        header_indices: headers
            .into_iter()
            .enumerate()
            .map(|(i, h)| (h.to_string(), i))
            .collect(),
        primary_key: None,
        foreign_keys: foreign_keys
            .into_iter()
            .map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
            .collect(),
    }
}

#[test]
fn test_create_graph() {
    let tables = vec![
        create_sample_dataframe("users", vec!["id", "name"], vec![]),
        create_sample_dataframe(
            "posts",
            vec!["id", "title", "user_id"],
            vec![("user_id", "users", "id")],
        ),
        create_sample_dataframe(
            "comments",
            vec!["id", "content", "post_id", "user_id"],
            vec![("post_id", "posts", "id"), ("user_id", "users", "id")],
        ),
    ];

    let graph = graph::create_graph(tables);

    assert_eq!(graph.node_count(), 3);
    assert_eq!(graph.edge_count(), 3);

    let node_names: HashSet<_> = graph.node_weights().map(|df| df.name.clone()).collect();
    assert_eq!(
        node_names,
        HashSet::from_iter(
            vec!["users", "posts", "comments"]
                .into_iter()
                .map(String::from)
        )
    );

    // Check edges
    let edges: Vec<_> = graph
        .edge_indices()
        .map(|e| {
            let (a, b) = graph.edge_endpoints(e).unwrap();
            let weight = graph.edge_weight(e).unwrap();
            (graph[a].name.clone(), graph[b].name.clone(), weight.clone())
        })
        .collect();

    assert!(edges.contains(&(
        "posts".to_string(),
        "users".to_string(),
        ("user_id".to_string(), "id".to_string())
    )));
    assert!(edges.contains(&(
        "comments".to_string(),
        "posts".to_string(),
        ("post_id".to_string(), "id".to_string())
    )));
    assert!(edges.contains(&(
        "comments".to_string(),
        "users".to_string(),
        ("user_id".to_string(), "id".to_string())
    )));
}

#[test]
fn test_serializable_graph() {
    let mut graph = UnGraph::new_undirected();
    let node1 = graph.add_node(create_sample_dataframe("users", vec!["id", "name"], vec![]));
    let node2 = graph.add_node(create_sample_dataframe(
        "posts",
        vec!["id", "title", "user_id"],
        vec![("user_id", "users", "id")],
    ));
    graph.add_edge(node1, node2, ("user_id".to_string(), "id".to_string()));

    let serializable = SerializableGraph::from(&graph);
    assert_eq!(serializable.nodes.len(), 2);
    assert_eq!(serializable.edges.len(), 1);

    let reconstructed = serializable.into_graph();
    assert_eq!(reconstructed.node_count(), 2);
    assert_eq!(reconstructed.edge_count(), 1);

    let node_names: HashSet<_> = reconstructed
        .node_weights()
        .map(|df| df.name.clone())
        .collect();
    assert_eq!(
        node_names,
        HashSet::from_iter(vec!["users", "posts"].into_iter().map(String::from))
    );

    let edge = reconstructed.edge_indices().next().unwrap();
    let (a, b) = reconstructed.edge_endpoints(edge).unwrap();
    let weight = reconstructed.edge_weight(edge).unwrap();
    assert_eq!(
        (
            reconstructed[a].name.clone(),
            reconstructed[b].name.clone(),
            weight.clone()
        ),
        (
            "users".to_string(),
            "posts".to_string(),
            ("user_id".to_string(), "id".to_string())
        )
    );
}

#[test]
fn test_write_dot_file() {
    let mut graph = UnGraph::new_undirected();
    let node1 = graph.add_node(create_sample_dataframe("users", vec!["id", "name"], vec![]));
    let node2 = graph.add_node(create_sample_dataframe(
        "posts",
        vec!["id", "title", "user_id"],
        vec![("user_id", "users", "id")],
    ));
    graph.add_edge(node1, node2, ("user_id".to_string(), "id".to_string()));

    let dot_content = graph::write_dot_file(&graph);

    // Check for basic structure
    assert!(dot_content.starts_with("graph G {"));
    assert!(dot_content.ends_with("}\n"));

    // Check for node declarations
    assert!(dot_content
        .contains("0 [label=<{<b><font point-size='16' color='red'>users</font></b>|id|name}>]"));
    assert!(dot_content.contains(
        "1 [label=<{<b><font point-size='16' color='red'>posts</font></b>|id|title|user_id}>]"
    ));

    // Check for edge declaration
    assert!(dot_content.contains("0 -- 1 [label=\"(user_id, id)\"]"));
}
