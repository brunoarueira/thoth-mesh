//! `thoth-mesh`: command-line client for publishing, subscribing, and
//! administering a thoth-mesh node.

use clap::Parser;
use thoth_mesh_cli::Cli;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    thoth_mesh_cli::run(Cli::parse()).await
}
