//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.

use clap::Parser;
use tracing_subscriber::EnvFilter;

/// Daemon that runs a thoth-mesh node: wires the local pub/sub broker
/// to a TCP transport.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Address to listen on.
    #[arg(long, default_value = thoth_mesh_node::DEFAULT_ADDR)]
    addr: String,

    /// Log level (or a full `tracing_subscriber::EnvFilter` directive,
    /// e.g. `thoth_mesh_node=debug`) to use when `RUST_LOG` isn't set.
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    // RUST_LOG wins when it's set, even if it fails to parse (in
    // which case we fall back to --log-level rather than silently
    // ignoring the environment); --log-level itself falls back to
    // "info" if it doesn't parse either.
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cli.log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    thoth_mesh_node::run(&cli.addr).await
}
