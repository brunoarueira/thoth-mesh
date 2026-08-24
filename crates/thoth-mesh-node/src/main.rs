//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.

use std::collections::HashSet;
use std::path::PathBuf;

use clap::Parser;
use thoth_mesh_node::TlsConfig;
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

    /// Address of a seed peer to dial on startup. Repeatable.
    #[arg(long = "peer")]
    peers: Vec<String>,

    /// Address to serve Prometheus-format metrics on (e.g.
    /// `127.0.0.1:9090`). Off by default - no metrics port is opened
    /// unless this is given.
    #[arg(long)]
    metrics_addr: Option<String>,

    /// This node's TLS certificate (PEM). Requires --tls-key and
    /// --tls-ca too - TLS is off (plaintext, as before) unless all
    /// three are given. See ADR-0016 and docs/OPERATIONS.md.
    #[arg(long, requires_all = ["tls_key", "tls_ca"])]
    tls_cert: Option<PathBuf>,

    /// This node's TLS private key (PEM). See --tls-cert.
    #[arg(long, requires_all = ["tls_cert", "tls_ca"])]
    tls_key: Option<PathBuf>,

    /// CA certificate (PEM) this node trusts to verify anyone else's
    /// TLS certificate. See --tls-cert.
    #[arg(long, requires_all = ["tls_cert", "tls_key"])]
    tls_ca: Option<PathBuf>,

    /// SHA-256 fingerprint (as printed by `openssl x509 -fingerprint
    /// -sha256`) of a peer certificate allowed to link as a peer.
    /// Repeatable. Requires --tls-cert/--tls-key/--tls-ca too - with
    /// none given, every peer link is allowed, unchanged from before
    /// this flag existed. See ADR-0017 and docs/OPERATIONS.md.
    #[arg(long = "allow-peer", requires = "tls_cert")]
    allow_peer: Vec<String>,
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

    let allowed_peers = if cli.allow_peer.is_empty() {
        None
    } else {
        let mut fingerprints = HashSet::new();
        for raw in &cli.allow_peer {
            let fingerprint = thoth_mesh_tls::parse_fingerprint(raw).map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("--allow-peer {raw:?}: {err}"),
                )
            })?;
            fingerprints.insert(fingerprint);
        }
        Some(fingerprints)
    };

    // clap's requires_all/requires already enforces all-or-nothing
    // across the three TLS flags (and that --allow-peer needs them
    // too); this just assembles them once that's guaranteed.
    let tls = match (cli.tls_cert, cli.tls_key, cli.tls_ca) {
        (Some(cert), Some(key), Some(ca)) => Some(TlsConfig {
            cert,
            key,
            ca,
            allowed_peers,
        }),
        _ => None,
    };

    thoth_mesh_node::run_with_tls(&cli.addr, cli.peers, cli.metrics_addr, tls).await
}
