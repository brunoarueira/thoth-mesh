//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.

mod config;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use thoth_mesh_core::Topic;
use thoth_mesh_node::{NodeOptions, PeerTopicFilter, RateLimitConfig, TlsConfig, TopicAcl};
use tracing_subscriber::EnvFilter;

/// Daemon that runs a thoth-mesh node: wires the local pub/sub broker
/// to a TCP transport.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Config file supplying defaults for every other flag below (see
    /// ADR-0054 and docs/OPERATIONS.md) - the conventional per-OS
    /// location if not given. A flag given on the command line always
    /// overrides the same key in the file.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Address to listen on. Defaults to `DEFAULT_ADDR` if given by
    /// neither this flag nor the config file.
    #[arg(long)]
    addr: Option<String>,

    /// Log level (or a full `tracing_subscriber::EnvFilter` directive,
    /// e.g. `thoth_mesh_node=debug`) to use when `RUST_LOG` isn't set.
    /// Defaults to "info" if given by neither this flag nor the config
    /// file.
    #[arg(long)]
    log_level: Option<String>,

    /// Address of a seed peer to dial on startup. Repeatable.
    #[arg(long = "peer")]
    peers: Vec<String>,

    /// Address to serve Prometheus-format metrics on (e.g.
    /// `127.0.0.1:9090`). Off by default - no metrics port is opened
    /// unless this is given.
    #[arg(long)]
    metrics_addr: Option<String>,

    /// Directory for the on-disk message store (a plain SQLite file,
    /// `messages.db`). With none given the node is fully in-memory and
    /// a restart loses everything, unchanged from before this flag
    /// existed. See ADR-0045 and docs/OPERATIONS.md.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// This node's TLS certificate (PEM). Requires --tls-key and
    /// --tls-ca too (from either this flag or the config file - see
    /// EffectiveConfig::merge, ADR-0054) - TLS is off (plaintext, as
    /// before) unless all three are given. See ADR-0016 and
    /// docs/OPERATIONS.md.
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// This node's TLS private key (PEM). See --tls-cert.
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// CA certificate (PEM) this node trusts to verify anyone else's
    /// TLS certificate. See --tls-cert.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// SHA-256 fingerprint (as printed by `openssl x509 -fingerprint
    /// -sha256`) of a peer certificate allowed to link as a peer.
    /// Repeatable. Requires --tls-cert/--tls-key/--tls-ca too - with
    /// none given, every peer link is allowed, unchanged from before
    /// this flag existed. See ADR-0017 and docs/OPERATIONS.md.
    #[arg(long = "allow-peer")]
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

    /// Per-topic peer-link publish/subscribe permission, same shape as
    /// --topic-acl - but checked against a connection already known to
    /// be a peer link instead of a client, and completely independent
    /// of --topic-acl (a peer link is never checked against that one,
    /// and a client is never checked against this one). With none
    /// given, every peer link can carry anything, unchanged from
    /// before this flag existed. See ADR-0020 and docs/OPERATIONS.md.
    #[arg(long = "peer-topic-acl")]
    peer_topic_acl: Vec<String>,

    /// Restricts which of this node's own aggregate topic interest a
    /// specific peer link is proactively told about, shaped
    /// `<fingerprint>|<topic>` (literal topic only, no wildcard).
    /// Repeatable. Distinct from --peer-topic-acl: that governs
    /// whether a peer is *permitted* to publish/subscribe to a topic
    /// if it explicitly asks; this governs what this node *volunteers
    /// unasked* via interest propagation - a peer can still explicitly
    /// ask for (and receive) anything --peer-topic-acl permits, even a
    /// topic this node would never have proactively announced. With
    /// none given, every peer link hears about everything, unchanged
    /// from before this flag existed; given at least once, a peer
    /// link with no entries of its own is told about nothing
    /// proactively. See ADR-0049 and docs/OPERATIONS.md.
    #[arg(long = "peer-topic-filter")]
    peer_topic_filter: Vec<String>,

    /// File containing a shared-secret bearer token a metrics scrape
    /// must present (as `Authorization: Bearer <token>`) to get the
    /// render. Requires --metrics-addr - with neither given, no
    /// metrics port opens at all; with --metrics-addr but no token
    /// file, any connection to it gets the render, unchanged from
    /// before this flag existed. See ADR-0019 and docs/OPERATIONS.md.
    #[arg(long = "metrics-token-file")]
    metrics_token_file: Option<PathBuf>,

    /// How long (in seconds) a message survives in the on-disk store
    /// (--data-dir) before the background sweep deletes it - not a
    /// dead-letter setting by itself, and not related to live/in-
    /// flight delivery at all; see --dead-letter-topic if that's what
    /// you're looking for. Requires --data-dir - there's no disk log
    /// to expire anything from otherwise. With none given, no
    /// age-based expiry runs; the on-disk log is only ever pruned by
    /// count, unchanged from before this flag existed. See ADR-0047
    /// and docs/OPERATIONS.md.
    #[arg(long = "persisted-message-ttl-secs")]
    persisted_message_ttl_secs: Option<u64>,

    /// A literal topic (not a wildcard) to republish an otherwise-
    /// unconsumed message to, as `<this>.<original topic>`, instead of
    /// just dropping it - a message aged past
    /// --persisted-message-ttl-secs, or an `ack: true` delivery that
    /// exhausts every redelivery attempt (ADR-0041). Standalone -
    /// works with no --data-dir/--persisted-message-ttl-secs at all
    /// for the latter source. With none given, both sources behave
    /// exactly as before this flag existed: silently dropped, only
    /// counted. See ADR-0047 and docs/OPERATIONS.md.
    #[arg(long = "dead-letter-topic")]
    dead_letter_topic: Option<String>,

    /// Sustained per-principal `Publish` rate, in messages/second - a
    /// client is identified the same way --topic-acl identifies one
    /// (TLS certificate fingerprint, or "anonymous" with none), and
    /// never applies to a peer link. Gates the whole feature: with
    /// none given, no rate limiting runs at all, unchanged from before
    /// this flag existed. See ADR-0051 and docs/OPERATIONS.md.
    #[arg(long = "publish-rate-limit-per-sec")]
    publish_rate_limit_per_sec: Option<u32>,

    /// Token bucket capacity backing --publish-rate-limit-per-sec - how
    /// far ahead of the sustained rate an idle principal can get
    /// before being throttled again. Requires
    /// --publish-rate-limit-per-sec; defaults to that flag's own value
    /// if omitted (a flat rate with no extra burst allowance). See
    /// ADR-0051 and docs/OPERATIONS.md.
    #[arg(long = "publish-rate-limit-burst")]
    publish_rate_limit_burst: Option<u32>,
}

/// Every `Cli` flag's actual, effective value once a config file
/// (ADR-0054) has been merged in - a CLI flag always wins over the
/// same key in the file, and `addr`/`log_level` fall back to their
/// built-in defaults if neither source gave them. Kept as its own
/// type, not `Cli` reused in place, so "was this actually given"
/// (`Option`/an empty `Vec`) can't leak past the one place that's
/// supposed to resolve it.
#[derive(Debug)]
struct EffectiveConfig {
    addr: String,
    log_level: String,
    peers: Vec<String>,
    metrics_addr: Option<String>,
    data_dir: Option<PathBuf>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    allow_peer: Vec<String>,
    topic_acl: Vec<String>,
    peer_topic_acl: Vec<String>,
    peer_topic_filter: Vec<String>,
    metrics_token_file: Option<PathBuf>,
    persisted_message_ttl_secs: Option<u64>,
    dead_letter_topic: Option<String>,
    publish_rate_limit_per_sec: Option<u32>,
    publish_rate_limit_burst: Option<u32>,
}

impl EffectiveConfig {
    /// Merges `cli` over `file`, field by field: a scalar is
    /// `cli.or(file).unwrap_or(built_in_default)` (only `addr`/
    /// `log_level` have one); a repeated flag is the CLI's own list in
    /// full if it's non-empty, otherwise the file's list untouched -
    /// never a union of both (see ADR-0054). Every cross-flag
    /// constraint below (the TLS trio, `--allow-peer` needing
    /// `--tls-cert`, and the rest) is `Cli`'s sole enforcement of it -
    /// deliberately *not* also `clap`'s own `requires`/`requires_all`,
    /// which only ever sees what was actually typed on the command
    /// line: it can't know a flag's dependency was satisfied by the
    /// config file instead, and would reject an otherwise-valid
    /// split-source invocation before this function ever ran. One
    /// check, here, covering every source uniformly.
    fn merge(cli: Cli, file: config::Config) -> std::io::Result<Self> {
        let tls_cert = cli.tls_cert.or(file.tls_cert);
        let tls_key = cli.tls_key.or(file.tls_key);
        let tls_ca = cli.tls_ca.or(file.tls_ca);
        if !(tls_cert.is_some() == tls_key.is_some() && tls_key.is_some() == tls_ca.is_some()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--tls-cert, --tls-key, and --tls-ca must be given together, whether from flags or the config file (see --config)",
            ));
        }

        let allow_peer = merge_list(cli.allow_peer, file.allow_peer);
        if !allow_peer.is_empty() && tls_cert.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--allow-peer requires --tls-cert/--tls-key/--tls-ca, whether from flags or the config file (see --config)",
            ));
        }

        let metrics_addr = cli.metrics_addr.or(file.metrics_addr);
        let metrics_token_file = cli.metrics_token_file.or(file.metrics_token_file);
        if metrics_token_file.is_some() && metrics_addr.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--metrics-token-file requires --metrics-addr, whether from flags or the config file (see --config)",
            ));
        }

        let data_dir = cli.data_dir.or(file.data_dir);
        let persisted_message_ttl_secs = cli
            .persisted_message_ttl_secs
            .or(file.persisted_message_ttl_secs);
        if persisted_message_ttl_secs.is_some() && data_dir.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--persisted-message-ttl-secs requires --data-dir, whether from flags or the config file (see --config)",
            ));
        }

        let publish_rate_limit_per_sec = cli
            .publish_rate_limit_per_sec
            .or(file.publish_rate_limit_per_sec);
        let publish_rate_limit_burst = cli
            .publish_rate_limit_burst
            .or(file.publish_rate_limit_burst);
        if publish_rate_limit_burst.is_some() && publish_rate_limit_per_sec.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--publish-rate-limit-burst requires --publish-rate-limit-per-sec, whether from flags or the config file (see --config)",
            ));
        }

        Ok(Self {
            addr: cli
                .addr
                .or(file.addr)
                .unwrap_or_else(|| thoth_mesh_node::DEFAULT_ADDR.to_owned()),
            log_level: cli
                .log_level
                .or(file.log_level)
                .unwrap_or_else(|| "info".to_owned()),
            peers: merge_list(cli.peers, file.peer),
            metrics_addr,
            data_dir,
            tls_cert,
            tls_key,
            tls_ca,
            allow_peer,
            topic_acl: merge_list(cli.topic_acl, file.topic_acl),
            peer_topic_acl: merge_list(cli.peer_topic_acl, file.peer_topic_acl),
            peer_topic_filter: merge_list(cli.peer_topic_filter, file.peer_topic_filter),
            metrics_token_file,
            persisted_message_ttl_secs,
            dead_letter_topic: cli.dead_letter_topic.or(file.dead_letter_topic),
            publish_rate_limit_per_sec,
            publish_rate_limit_burst,
        })
    }
}

/// `cli`'s own list in full if it's non-empty (the flag was given at
/// least once), otherwise `file`'s list untouched - never a union of
/// both. See `EffectiveConfig::merge`.
fn merge_list(cli: Vec<String>, file: Vec<String>) -> Vec<String> {
    if cli.is_empty() { file } else { cli }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let file = config::load(cli.config.as_deref())?;
    let cli = EffectiveConfig::merge(cli, file)?;

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

    // EffectiveConfig::merge already enforces all-or-nothing across
    // the three TLS flags (and that --allow-peer needs them too, from
    // either source); this just assembles them once that's guaranteed.
    let tls = match (cli.tls_cert, cli.tls_key, cli.tls_ca) {
        (Some(cert), Some(key), Some(ca)) => Some(TlsConfig {
            cert,
            key,
            ca,
            allowed_peers,
        }),
        _ => None,
    };

    let topic_acl = parse_topic_acl("--topic-acl", &cli.topic_acl)?;
    let peer_topic_acl = parse_topic_acl("--peer-topic-acl", &cli.peer_topic_acl)?;
    let peer_topic_filter = if cli.peer_topic_filter.is_empty() {
        None
    } else {
        Some(
            PeerTopicFilter::parse(cli.peer_topic_filter.iter().map(String::as_str)).map_err(
                |err| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("--peer-topic-filter {err}"),
                    )
                },
            )?,
        )
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

    let dead_letter_topic = match &cli.dead_letter_topic {
        None => None,
        Some(raw) => Some(raw.parse::<Topic>().map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("--dead-letter-topic {raw:?}: {err}"),
            )
        })?),
    };

    // 0 isn't "disabled" (omitting the flag entirely already means
    // that) - it's a token bucket that never refills and/or never
    // holds a token, silently black-holing every publish from every
    // principal. Reject it outright rather than let an operator
    // typing 0 get a full publish outage with no clearer signal than
    // a stream of per-message Error replies.
    let rate_limit = match (cli.publish_rate_limit_per_sec, cli.publish_rate_limit_burst) {
        (None, _) => None,
        (Some(0), _) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--publish-rate-limit-per-sec must be at least 1 (omit the flag entirely to disable rate limiting)",
            ));
        }
        (Some(_), Some(0)) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--publish-rate-limit-burst must be at least 1",
            ));
        }
        (Some(per_sec), burst) => Some(RateLimitConfig {
            per_sec,
            burst: burst.unwrap_or(per_sec),
        }),
    };

    let options = NodeOptions {
        tls,
        topic_acl,
        peer_topic_acl,
        peer_topic_filter,
        data_dir: cli.data_dir,
        persisted_message_ttl: cli
            .persisted_message_ttl_secs
            .map(std::time::Duration::from_secs),
        dead_letter_topic,
        rate_limit,
    };

    thoth_mesh_node::run_with_tls(
        &cli.addr,
        cli.peers,
        cli.metrics_addr,
        options,
        metrics_token,
    )
    .await
}

/// Parses a repeatable `<flag> <principal>|<action>|<topic>` list
/// (`--topic-acl`/`--peer-topic-acl`) into a [`TopicAcl`], or `None`
/// if `entries` is empty - the "unchanged from before this flag
/// existed" case both flags share. `flag` is included in a parse
/// error so it's clear which of the two was invalid.
fn parse_topic_acl(flag: &str, entries: &[String]) -> std::io::Result<Option<TopicAcl>> {
    if entries.is_empty() {
        return Ok(None);
    }
    let acl = TopicAcl::parse(entries.iter().map(String::as_str)).map_err(|err| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{flag} {err}"))
    })?;
    Ok(Some(acl))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `Cli` field left at its "nothing given" value - a
    /// starting point tests override just the fields they care about,
    /// via struct-update syntax (`Cli { addr: Some(...), ..empty_cli()
    /// }`).
    fn empty_cli() -> Cli {
        Cli {
            config: None,
            addr: None,
            log_level: None,
            peers: Vec::new(),
            metrics_addr: None,
            data_dir: None,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            allow_peer: Vec::new(),
            topic_acl: Vec::new(),
            peer_topic_acl: Vec::new(),
            peer_topic_filter: Vec::new(),
            metrics_token_file: None,
            persisted_message_ttl_secs: None,
            dead_letter_topic: None,
            publish_rate_limit_per_sec: None,
            publish_rate_limit_burst: None,
        }
    }

    #[test]
    fn scalar_falls_back_to_the_built_in_default_with_neither_source() {
        let effective = EffectiveConfig::merge(empty_cli(), config::Config::default()).unwrap();
        assert_eq!(effective.addr, thoth_mesh_node::DEFAULT_ADDR);
        assert_eq!(effective.log_level, "info");
    }

    #[test]
    fn scalar_uses_the_config_file_when_the_cli_omits_it() {
        let file = config::Config {
            addr: Some("127.0.0.2:49500".to_owned()),
            ..Default::default()
        };
        let effective = EffectiveConfig::merge(empty_cli(), file).unwrap();
        assert_eq!(effective.addr, "127.0.0.2:49500");
    }

    #[test]
    fn scalar_cli_flag_wins_over_the_config_file() {
        let cli = Cli {
            addr: Some("127.0.0.3:49500".to_owned()),
            ..empty_cli()
        };
        let file = config::Config {
            addr: Some("127.0.0.2:49500".to_owned()),
            ..Default::default()
        };
        let effective = EffectiveConfig::merge(cli, file).unwrap();
        assert_eq!(effective.addr, "127.0.0.3:49500");
    }

    #[test]
    fn a_non_empty_cli_list_replaces_the_config_files_list_entirely() {
        let cli = Cli {
            topic_acl: vec!["anonymous|pub|only.this".to_owned()],
            ..empty_cli()
        };
        let file = config::Config {
            topic_acl: vec!["anonymous|pub|from.file".to_owned()],
            ..Default::default()
        };
        let effective = EffectiveConfig::merge(cli, file).unwrap();
        assert_eq!(effective.topic_acl, vec!["anonymous|pub|only.this"]);
    }

    #[test]
    fn an_empty_cli_list_falls_back_to_the_config_files_list() {
        let file = config::Config {
            topic_acl: vec!["anonymous|pub|from.file".to_owned()],
            ..Default::default()
        };
        let effective = EffectiveConfig::merge(empty_cli(), file).unwrap();
        assert_eq!(effective.topic_acl, vec!["anonymous|pub|from.file"]);
    }

    #[test]
    fn the_tls_trio_split_across_cli_and_file_is_still_all_or_nothing() {
        let cli = Cli {
            tls_key: Some(PathBuf::from("key.pem")),
            ..empty_cli()
        };
        let file = config::Config {
            tls_cert: Some(PathBuf::from("cert.pem")),
            ..Default::default()
        };
        let err = EffectiveConfig::merge(cli, file).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn the_tls_trio_fully_from_the_config_file_is_accepted() {
        let file = config::Config {
            tls_cert: Some(PathBuf::from("cert.pem")),
            tls_key: Some(PathBuf::from("key.pem")),
            tls_ca: Some(PathBuf::from("ca.pem")),
            ..Default::default()
        };
        assert!(EffectiveConfig::merge(empty_cli(), file).is_ok());
    }

    #[test]
    fn allow_peer_requires_tls_cert_even_when_each_comes_from_a_different_source() {
        let cli = Cli {
            allow_peer: vec!["AA:BB".to_owned()],
            ..empty_cli()
        };
        // No tls_cert anywhere - neither CLI nor file.
        let err = EffectiveConfig::merge(cli, config::Config::default()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn metrics_token_file_requires_metrics_addr_even_when_each_comes_from_a_different_source() {
        let cli = Cli {
            metrics_token_file: Some(PathBuf::from("token")),
            ..empty_cli()
        };
        let file = config::Config::default(); // no metrics_addr anywhere
        let err = EffectiveConfig::merge(cli, file).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn persisted_message_ttl_secs_requires_data_dir_even_when_each_comes_from_a_different_source() {
        let cli = Cli {
            persisted_message_ttl_secs: Some(60),
            ..empty_cli()
        };
        let err = EffectiveConfig::merge(cli, config::Config::default()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn publish_rate_limit_burst_requires_per_sec_even_when_each_comes_from_a_different_source() {
        let file = config::Config {
            publish_rate_limit_burst: Some(200),
            ..Default::default()
        };
        // per_sec given nowhere.
        let err = EffectiveConfig::merge(empty_cli(), file).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn a_fully_valid_merge_across_both_sources_succeeds() {
        let cli = Cli {
            addr: Some("127.0.0.1:49500".to_owned()),
            tls_key: Some(PathBuf::from("key.pem")),
            ..empty_cli()
        };
        let file = config::Config {
            tls_cert: Some(PathBuf::from("cert.pem")),
            tls_ca: Some(PathBuf::from("ca.pem")),
            allow_peer: vec!["AA:BB".to_owned()],
            ..Default::default()
        };
        let effective = EffectiveConfig::merge(cli, file).unwrap();
        assert_eq!(effective.addr, "127.0.0.1:49500");
        assert_eq!(effective.allow_peer, vec!["AA:BB"]);
    }

    /// The scenario a real invocation actually hits: `--allow-peer`
    /// given as a flag, with the TLS trio it depends on supplied
    /// entirely by the config file. Exercising this only through
    /// `merge` directly (as every other test here does) wouldn't have
    /// caught that `Cli`'s own clap-level `requires` attributes used
    /// to reject this exact invocation before `merge` ever ran, since
    /// clap has no visibility into the file - see the git history of
    /// this test for the actual `thoth-mesh-node` invocation that
    /// reproduced it.
    #[test]
    fn cli_flag_whose_requirement_is_satisfied_only_by_the_config_file_is_accepted() {
        let cli = Cli {
            allow_peer: vec!["AA:BB".to_owned()],
            ..empty_cli()
        };
        let file = config::Config {
            tls_cert: Some(PathBuf::from("cert.pem")),
            tls_key: Some(PathBuf::from("key.pem")),
            tls_ca: Some(PathBuf::from("ca.pem")),
            ..Default::default()
        };
        let effective = EffectiveConfig::merge(cli, file).unwrap();
        assert_eq!(effective.allow_peer, vec!["AA:BB"]);
        assert!(effective.tls_cert.is_some());
    }

    /// The actual layer the original bug lived in: `Cli::parse`
    /// itself, not `merge`. Every test above calls `merge` directly,
    /// which can't see a clap-level `requires`/`requires_all`
    /// rejecting the invocation before `merge` ever runs - this is
    /// the one that would have caught it. `--allow-peer` alone (no
    /// `--tls-cert`/`--tls-key`/`--tls-ca` flags at all) must parse
    /// cleanly now; whether it's actually a *valid* combination is
    /// `merge`'s job, covered above and elsewhere.
    #[test]
    fn cli_parsing_no_longer_rejects_a_flag_whose_requirement_might_come_from_the_file() {
        let cli = Cli::try_parse_from([
            "thoth-mesh-node",
            "--allow-peer",
            "AA:BB",
            "--metrics-token-file",
            "token.txt",
            "--persisted-message-ttl-secs",
            "60",
            "--publish-rate-limit-burst",
            "200",
        ])
        .unwrap();
        assert_eq!(cli.allow_peer, vec!["AA:BB"]);
    }
}
