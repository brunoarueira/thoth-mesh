//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Level is controlled by `RUST_LOG` (e.g. `RUST_LOG=debug`); falls
    // back to `info` when unset or invalid. A `--log-level` CLI flag
    // will layer on top of this once node configuration lands.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    thoth_mesh_node::run(thoth_mesh_node::DEFAULT_ADDR).await
}
