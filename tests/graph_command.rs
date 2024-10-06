use csvg::{
    commands::graph::{
        find_join_columns, find_node, find_shortest_path, update_dataframe_after_join,
    },
    csv::DataFrame,
};
use petgraph::graph::UnGraph;
use std::collections::HashMap;

// Helper function to create a mock graph
fn create_mock_graph() -> UnGraph<DataFrame, (String, String)> {
    let mut g = UnGraph::new_undirected();
    let df1 = DataFrame {
        name: "table1".to_string(),
        headers: vec!["id".to_string(), "name".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("id".to_string()),
        foreign_keys: vec![],
    };
    let df2 = DataFrame {
        name: "table2".to_string(),
        headers: vec!["id".to_string(), "value".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("id".to_string()),
        foreign_keys: vec![("id".to_string(), "table1".to_string(), "id".to_string())],
    };
    let df3 = DataFrame {
        name: "table3".to_string(),
        headers: vec!["id".to_string(), "description".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("id".to_string()),
        foreign_keys: vec![("id".to_string(), "table2".to_string(), "id".to_string())],
    };
    let n1 = g.add_node(df1);
    let n2 = g.add_node(df2);
    let n3 = g.add_node(df3);
    g.add_edge(n1, n2, ("id".to_string(), "id".to_string()));
    g.add_edge(n2, n3, ("id".to_string(), "id".to_string()));
    g
}

#[test]
fn test_find_node() {
    let g = create_mock_graph();
    assert!(find_node(&g, "table1").is_ok());
    assert!(find_node(&g, "table2").is_ok());
    assert!(find_node(&g, "table3").is_ok());
    assert!(find_node(&g, "nonexistent").is_err());
}

#[test]
fn test_find_shortest_path() {
    let g = create_mock_graph();
    let start = find_node(&g, "table1").unwrap();
    let end = find_node(&g, "table3").unwrap();
    let path = find_shortest_path(&g, start, end).unwrap();
    assert_eq!(path.len(), 3);
    assert_eq!(g[path[0]].name, "table1");
    assert_eq!(g[path[1]].name, "table2");
    assert_eq!(g[path[2]].name, "table3");
}

#[test]
fn test_find_join_columns() {
    let df1 = DataFrame {
        name: "table1".to_string(),
        headers: vec!["id".to_string(), "name".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("id".to_string()),
        foreign_keys: vec![],
    };
    let df2 = DataFrame {
        name: "table2".to_string(),
        headers: vec!["id".to_string(), "value".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("id".to_string()),
        foreign_keys: vec![("id".to_string(), "table1".to_string(), "id".to_string())],
    };
    let (left_col, right_col) = find_join_columns(&df1, &df2).unwrap();
    assert_eq!(left_col, "id");
    assert_eq!(right_col, "id");

    let df3 = DataFrame {
        name: "table3".to_string(),
        headers: vec!["code".to_string(), "description".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("code".to_string()),
        foreign_keys: vec![],
    };
    assert!(find_join_columns(&df1, &df3).is_err());
}

#[test]
fn test_update_dataframe_after_join() {
    let left_df = DataFrame {
        name: "table1".to_string(),
        headers: vec!["id".to_string(), "name".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("id".to_string()),
        foreign_keys: vec![],
    };
    let right_df = DataFrame {
        name: "table2".to_string(),
        headers: vec!["id".to_string(), "value".to_string()],
        header_indices: HashMap::new(),
        primary_key: Some("id".to_string()),
        foreign_keys: vec![("id".to_string(), "table1".to_string(), "id".to_string())],
    };
    let joined_df = update_dataframe_after_join(&left_df, &right_df, "id", "id");
    assert_eq!(joined_df.headers, vec!["id", "name", "value"]);
    assert_eq!(joined_df.primary_key, Some("id".to_string()));
    assert!(joined_df.foreign_keys.is_empty());
}
