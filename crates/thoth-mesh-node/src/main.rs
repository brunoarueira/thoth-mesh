//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use thoth_mesh_node::{TlsConfig, TopicAcl};
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

    /// Per-topic client publish/subscribe permission, shaped
    /// `<principal>|<action>|<topic>` (principal: a fingerprint like
    /// --allow-peer's, or "anonymous"; action: "pub", "sub", or
    /// "pubsub"). Repeatable. With none given, every client can
    /// publish/subscribe to anything, unchanged from before this flag
    /// existed; given at least once, only listed combinations are
    /// allowed. See ADR-0018 and docs/OPERATIONS.md.
    #[arg(long = "topic-acl")]
    topic_acl: Vec<String>,

    /// File containing a shared-secret bearer token a metrics scrape
    /// must present (as `Authorization: Bearer <token>`) to get the
    /// render. Requires --metrics-addr - with neither given, no
    /// metrics port opens at all; with --metrics-addr but no token
    /// file, any connection to it gets the render, unchanged from
    /// before this flag existed. See ADR-0019 and docs/OPERATIONS.md.
    #[arg(long = "metrics-token-file", requires = "metrics_addr")]
    metrics_token_file: Option<PathBuf>,
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

    let topic_acl = if cli.topic_acl.is_empty() {
        None
    } else {
        let acl = TopicAcl::parse(cli.topic_acl.iter().map(String::as_str)).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("--topic-acl {err}"),
            )
        })?;
        Some(acl)
    };

    let metrics_token = match &cli.metrics_token_file {
        None => None,
        Some(path) => {
            let raw = std::fs::read_to_string(path).map_err(|err| {
                std::io::Error::new(err.kind(), format!("--metrics-token-file {path:?}: {err}"))
            })?;
            let token = raw.trim();
            if token.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("--metrics-token-file {path:?}: file is empty"),
                ));
            }
            Some(Arc::from(token))
        }
    };

    thoth_mesh_node::run_with_tls(
        &cli.addr,
        cli.peers,
        cli.metrics_addr,
        tls,
        topic_acl,
        metrics_token,
    )
    .await
}
