use clap::{command, Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    author,
    version,
    about = "SQL schema analysis and CSV manipulation tool",
    long_about = "csvgraph is a command-line tool designed for SQL schema analysis and CSV file manipulation. It allows you to create graphs from SQL schemas, find the shortest paths between tables, and perform various CSV file operations."
)]
pub struct Cli {
    /// Specify output file for the result
    #[arg(short, long, global = true)]
    pub output: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize csvgraph configuration
    #[command(alias = "I", alias = "initialize", alias = "-i")]
    Init(InitArgs),

    /// Perform graph operations on SQL schemas
    #[command(alias = "G")]
    Graph(GraphArgs),

    /// Handle CSV files
    #[command()]
    Csv(CsvArgs),

    /// Show path to config directory
    #[command()]
    Path,
}

#[derive(Args)]
pub struct InitArgs {
    /// Overwrite existing config
    #[arg(short, long)]
    pub force: bool,
}

#[derive(Args)]
pub struct GraphArgs {
    /// Force regeneration of the graph
    #[arg(short, long, alias = "regen")]
    pub regenerate: bool,

    #[command(subcommand)]
    pub subcommand: Option<GraphSubcommands>,
}

#[derive(Subcommand)]
pub enum GraphSubcommands {
    /// Create a graph from SQL schema
    #[command()]
    Create {
        /// Path to SQL schema file
        #[arg()]
        schema: String,
    },

    /// Find the shortest path between two tables
    #[command(alias = "sp", alias = "shortest")]
    ShortestPath {
        /// Source table
        #[arg()]
        from: String,
        /// Destination table
        #[arg()]
        to: String,
    },

    /// Create a minimum spanning tree from the schema
    #[command()]
    Mst,

    /// Display the graph structure
    #[command()]
    Display {
        /// Output format (dot, json, text)
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Join two CSV files
    #[command()]
    Join {
        /// First CSV file
        #[arg()]
        left_table: String,
        /// Second CSV file
        #[arg()]
        right_table: String,
        /// Join type (inner, left, right, full)
        #[arg(short, long, default_value = "inner")]
        r#type: String,
    },
}

#[derive(Args)]
pub struct CsvArgs {
    #[command(subcommand)]
    pub subcommand: CsvSubcommands,
}

#[derive(Subcommand)]
pub enum CsvSubcommands {
    /// Display the first n rows of a CSV file
    #[command()]
    Head {
        /// Input CSV file
        #[arg(help = "Input CSV file")]
        file: String,
        /// Number of lines to display
        #[arg(short, long, default_value = "10")]
        lines: usize,
    },

    /// Display the last n rows of a CSV file
    #[command()]
    Tail {
        /// Input CSV file
        #[arg(help = "Input CSV file")]
        file: String,
        /// Number of lines to display
        #[arg(short, long, default_value = "10")]
        lines: usize,
    },

    /// Join two CSV files
    #[command()]
    Join {
        /// First CSV file
        #[arg()]
        file1: String,
        /// Second CSV file
        #[arg()]
        file2: String,
        /// Left table column
        #[arg()]
        left_column: String,
        /// Right table column
        #[arg()]
        right_column: String,
        /// Join type (inner, left, right, full)
        #[arg(short, long, default_value = "inner")]
        r#type: String,
    },

    /// Concatenate CSV files vertically
    #[command()]
    Concat {
        /// CSV files to concatenate
        #[arg()]
        files: Vec<String>,
    },

    /// Select specific columns from a CSV file
    #[command()]
    Select {
        /// Input CSV file
        #[arg()]
        file: String,
        /// Columns to select
        #[arg()]
        columns: Vec<String>,
    },

    /// Drop (Remove) specific columns from a CSV file
    #[command()]
    Drop {
        /// Input CSV file
        #[arg()]
        file: String,

        /// Columns to drop
        #[arg()]
        columns: Vec<String>,
    },
}

pub fn parse_args() -> Cli {
    Cli::parse()
}
