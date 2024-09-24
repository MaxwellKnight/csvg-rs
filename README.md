# CSVGraph

**Note: This is an ongoing project and is actively being developed. Features and documentation may change frequently.**

csvg is a versatile command-line tool designed for SQL schema analysis and CSV file manipulation. It allows you to create graphs from SQL schemas, find the shortest paths between tables, and perform various CSV file operations.

## Current Features

- CSV file handling:
  - Display first or last n rows (head/tail)
  - Join CSV files
  - Concatenate CSV files vertically
  - Select specific columns
  - Drop (remove) specific columns
- SQL schema parsing and graph operations:
  - Create graph from SQL schema
  - Find shortest path between tables
  - Generate minimum spanning tree 
  - Display graph structure
- Graph visualization of database relationships
- Configuration management
- Performance optimization through graph caching

## Installation

*To be added as the project progresses*

## Usage

### CSV Handling

```bash
csvg csv head <FILE> [-l <LINES>]
csvg csv tail <FILE> [-l <LINES>]
csvg csv join <FILE1> <FILE2> <LEFT_COLUMN> <RIGHT_COLUMN> [-t <TYPE>]
csvg csv concat <FILES>...
csvg csv select <FILE> <COLUMNS>...
csvg csv drop <FILE> <COLUMNS>...
```

### SQL Schema Operations

```bash
csvg graph create [<SCHEMA>]
csvg graph shortest-path <FROM> <TO>
csvg graph join <LEFT_TABLE> <RIGHT_TABLE>
csvg graph mst
csvg graph display [-f <FORMAT>]
```

### Configuration

```bash
csvg init [-f]
csvg path
```

## Configuration Folder

csvg uses a configuration folder (`.csvgraph`) to store settings and cache graph data. This folder is created in the current working directory when you run `csvg init`.

### Structure

The configuration folder contains:
- `config.json`: Stores user settings and preferences.
- `graph.json`: Caches the generated graph data for faster subsequent operations.

### Usage

1. **Initializing the Config**:
   Run `csvg init` to create the config folder and initial settings.

2. **Forcing Reinitialization**:
   Use `csvg init -f` to overwrite existing configuration.

3. **Viewing Config Path**:
   `csvg path` shows the path to the configuration folder.

4. **Config File Contents**:
   The `config.json` file contains:
   - `output_path`: Directory for generated files.
   - `source_path`: Directory containing source CSV files.
   - `output_file`: Default output file for join operations.
   - Other settings as defined in the `Config` struct.

5. **Graph Caching**:
   The `graph.json` file caches the graph structure, improving performance for repeated operations on the same schema.

### Modifying Configuration

You can manually edit the `config.json` file to change settings. Alternatively, use the `csvg init -f` command to reset to default values.

## Project Structure

- `cli`: Command-line interface parsing
- `config`: Configuration management
- `csv`: CSV file handling
- `graph`: Graph creation and operations
- `sql`: SQL schema processing

## Recent Improvements

- Enhanced join operations with support for joining multiple tables along the shortest path
- Improved error handling and reporting
- Updated DataFrame structure to better handle join operations
- Optimized graph caching for faster subsequent operations

## Extensibility

csvg is designed to be easily extensible. Here are some areas where you can contribute or expand the project:

1. **Additional SQL Dialects**: Extend the SQL parser to support more database-specific syntax.
2. **Enhanced Visualization**: Improve the graph generation with more detailed node and edge representations.
3. **Data Analysis**: Implement statistical analysis features for CSV data.
4. **Database Integration**: Add functionality to interact directly with database systems.
5. **GUI Development**: Create a graphical user interface for the tool.
6. **Configuration Options**: Expand the configuration system with more customizable settings.
7. **Graph Algorithms**: Implement additional graph algorithms for schema analysis.
9. **Performance Optimization**: Further optimize large dataset handling and join operations

## Contributing

Contributions are welcome! As this is an ongoing project, please check the issues tab for current tasks or propose new features through pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

*To be determined*

## Contact

*Project maintainer information to be added*
