use crate::sql::parse_sql;
use std::error::Error;

#[test]
fn test_parse_sql_with_alter_table() -> Result<(), Box<dyn Error>> {
    let sql = r#"
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(255)
        );
        ALTER TABLE users ADD CONSTRAINT fk_company
            FOREIGN KEY (company_id) REFERENCES companies(id);
    "#;

    let tables = parse_sql(sql)?;

    assert_eq!(tables.len(), 1);
    let table = &tables[0];
    assert_eq!(table.name, "users");

    let expected_headers = vec!["id".to_string(), "name".to_string()];
    assert_eq!(table.headers, expected_headers);

    assert_eq!(table.primary_key, Some("id".to_string()));
    let expected_foreign_keys = vec![(
        "company_id".to_string(),
        "companies".to_string(),
        "id".to_string(),
    )];
    assert_eq!(table.foreign_keys, expected_foreign_keys);

    Ok(())
}

#[test]
fn test_parse_sql_with_composite_primary_key() -> Result<(), Box<dyn Error>> {
    let sql = r#"
        CREATE TABLE order_items (
            order_id INT,
            product_id INT,
            quantity INT,
            PRIMARY KEY (order_id, product_id)
        );
    "#;

    let tables = parse_sql(sql)?;

    assert_eq!(tables.len(), 1);
    let table = &tables[0];
    assert_eq!(table.name, "order_items");

    let expected_headers = vec![
        "order_id".to_string(),
        "product_id".to_string(),
        "quantity".to_string(),
    ];
    assert_eq!(table.headers, expected_headers);
    assert_eq!(table.primary_key, None);
    Ok(())
}

#[test]
fn test_parse_sql_with_multiple_foreign_keys() -> Result<(), Box<dyn Error>> {
    let sql = r#"
        CREATE TABLE orders (
            id INT PRIMARY KEY,
            user_id INT,
            product_id INT,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
    "#;

    let tables = parse_sql(sql)?;

    assert_eq!(tables.len(), 1);
    let table = &tables[0];
    assert_eq!(table.name, "orders");

    let expected_headers = vec![
        "id".to_string(),
        "user_id".to_string(),
        "product_id".to_string(),
    ];
    assert_eq!(table.headers, expected_headers);

    assert_eq!(table.primary_key, Some("id".to_string()));
    let expected_foreign_keys = vec![
        ("user_id".to_string(), "users".to_string(), "id".to_string()),
        (
            "product_id".to_string(),
            "products".to_string(),
            "id".to_string(),
        ),
    ];
    assert_eq!(table.foreign_keys, expected_foreign_keys);

    Ok(())
}

#[test]
fn test_parse_sql_with_comments() -> Result<(), Box<dyn Error>> {
    let sql = r#"
        -- This is a comment
        CREATE TABLE users (
            id INT PRIMARY KEY,
            /* This is a
               multi-line comment */
            name VARCHAR(255)
        );
    "#;

    let tables = parse_sql(sql)?;

    assert_eq!(tables.len(), 1);
    let table = &tables[0];
    assert_eq!(table.name, "users");

    let expected_headers = vec!["id".to_string(), "name".to_string()];
    assert_eq!(table.headers, expected_headers);

    assert_eq!(table.primary_key, Some("id".to_string()));

    Ok(())
}
