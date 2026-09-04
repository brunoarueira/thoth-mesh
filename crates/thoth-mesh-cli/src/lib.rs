//! `thoth-mesh`: command-line client for publishing, subscribing, and
//! administering a thoth-mesh node.
//!
//! v1 is intentionally minimal (see issue #13): no output formatting
//! options. Admin commands are deferred until the node actually has
//! an admin protocol to talk to. Connection options (`--addr`,
//! `--tls-*`) can be set once via a config file instead of repeated
//! flags - see ADR-0034.

mod config;

use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use thoth_mesh_core::{
    Envelope, MessageId, MessageKind, PeerId, Topic, TopicFilter, async_framing,
};
use thoth_mesh_tls::{MaybeTlsStream, TlsConnector, client_config, load_certs, load_private_key};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub use thoth_mesh_core::DEFAULT_ADDR;

#[derive(Parser, Debug)]
// Explicit name: this crate is thoth-mesh-cli, but the binary it
// builds - and what `--help`/`--version` and, since ADR-0036,
// generated completions should actually say - is `thoth-mesh`. Left
// implicit, clap defaults to CARGO_PKG_NAME (the crate name) instead.
#[command(name = "thoth-mesh", version, about)]
pub struct Cli {
    /// Node address to connect to. Falls back to the config file's
    /// `addr` (see --config), then to the built-in default, if not
    /// given. See ADR-0034.
    #[arg(long, global = true)]
    pub addr: Option<String>,

    /// Config file to read connection defaults from, in place of the
    /// conventional per-OS location (`~/.config/thoth-mesh/config.toml`
    /// on Linux). See ADR-0034.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// CA certificate (PEM) to trust for verifying the node's TLS
    /// certificate. Enables TLS for this connection - without it, the
    /// connection is plaintext, as before. See ADR-0016.
    #[arg(long, global = true)]
    pub tls_ca: Option<PathBuf>,

    /// This client's own TLS certificate (PEM), to identify itself to
    /// the node. Optional even with --tls-ca set - a plain client is
    /// still the `anonymous` principal for any `--topic-acl` on the
    /// node it's talking to (see ADR-0018), which may or may not be
    /// enough depending on how that node's configured. Requires
    /// --tls-key.
    #[arg(long, global = true, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// This client's own TLS private key (PEM). See --tls-cert.
    #[arg(long, global = true, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Publish a payload to a topic and exit.
    Publish {
        /// Topic to publish to.
        topic: String,
        /// Payload to send, as UTF-8 text - or `-` to read the
        /// payload as raw bytes from stdin instead, the only way to
        /// send a binary payload, or one too large for a CLI argument.
        /// See ADR-0035.
        payload: String,
    },
    /// Subscribe to one or more topic filters and print delivered
    /// messages until interrupted (Ctrl-C).
    Subscribe {
        /// Topic filter(s) to subscribe to - each a literal topic
        /// name, or an MQTT-style wildcard pattern (`+` matches one
        /// segment, a trailing `#` matches the rest; see ADR-0022).
        /// Give more than one to watch several filters over one
        /// connection (see ADR-0033).
        #[arg(required = true)]
        filters: Vec<String>,
        /// How to print delivered messages: `text` (default,
        /// human-readable, lossy for a non-UTF-8 payload) or `raw`
        /// (exact payload bytes to stdout, binary-safe - see
        /// ADR-0035).
        #[arg(long, value_enum, default_value_t = OutputMode::Text)]
        output: OutputMode,
    },
    /// Print a tab-completion script for `shell` to stdout, then exit.
    /// See ADR-0036 for how to install the result.
    Completions { shell: clap_complete::Shell },
}

/// `subscribe --output <MODE>`. See ADR-0035.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum OutputMode {
    /// `[topic] {payload}`, decoded as UTF-8 (lossy: an invalid byte
    /// sequence is replaced with U+FFFD). The default.
    Text,
    /// Every delivered message's raw payload bytes, written to stdout
    /// back-to-back with nothing else - no topic label, no separator
    /// between messages. Binary-safe. The `Subscribed to ...` banner
    /// and a per-message `[topic] N bytes` note go to stderr instead,
    /// so stdout stays exactly the payload bytes.
    Raw,
}

/// Runs the CLI: connects to `cli.addr` and executes `cli.command`.
pub async fn run(cli: Cli) -> std::io::Result<()> {
    // Generating a completion script touches neither the network nor
    // the config file every other command needs - handled before any
    // of that setup runs, not after. See ADR-0036.
    if let Command::Completions { shell } = cli.command {
        return print_completions(shell, &mut std::io::stdout());
    }

    // Validated up front, before dialing anything - a malformed topic
    // or filter should fail immediately, not only after a possibly
    // slow connect/TLS handshake.
    match &cli.command {
        Command::Publish { topic, .. } => {
            parse_topic(topic)?;
        }
        Command::Subscribe { filters, .. } => {
            for filter in filters {
                parse_filter(filter)?;
            }
        }
        Command::Completions { .. } => unreachable!("returned above"),
    }

    let config = config::load(cli.config.as_deref())?;
    let conn_opts = ConnectionOptions::merge(&cli, config)?;

    let connector = build_connector(&conn_opts)?;
    let tcp = TcpStream::connect(&conn_opts.addr).await?;
    let stream = match &connector {
        Some(connector) => MaybeTlsStream::connect(connector, tcp, &conn_opts.addr)
            .await
            .map_err(std::io::Error::other)?,
        None => MaybeTlsStream::Plain(tcp),
    };
    let mut conn = stream.compat();
    let sender = PeerId::new();

    match cli.command {
        Command::Publish { topic, payload } => {
            let topic = parse_topic(&topic)?;
            let payload = read_payload(&payload, tokio::io::stdin()).await?;
            let envelope = Envelope::new(sender, MessageKind::Publish { topic, payload });
            send(&mut conn, &envelope).await
        }
        Command::Subscribe { filters, output } => {
            let filters = filters
                .iter()
                .map(|filter| parse_filter(filter))
                .collect::<std::io::Result<Vec<_>>>()?;
            subscribe_and_print(&mut conn, sender, filters, output).await
        }
        Command::Completions { .. } => unreachable!("returned above"),
    }
}

/// Writes `shell`'s completion script for this binary to `out`. See
/// ADR-0036.
fn print_completions(shell: clap_complete::Shell, out: &mut impl Write) -> std::io::Result<()> {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_owned();
    clap_complete::generate(shell, &mut cmd, name, out);
    Ok(())
}

/// Resolves `arg` (`publish`'s `payload` argument) to the actual bytes
/// to send: `arg` itself, UTF-8 encoded, unless `arg` is exactly `-`,
/// in which case `stdin` is read to EOF instead and used as-is - not
/// decoded as UTF-8, which is what makes a binary payload possible at
/// all. Takes `stdin` as a parameter (rather than calling
/// `tokio::io::stdin()` itself) so tests can exercise the `-` path
/// against an in-memory buffer. See ADR-0035.
async fn read_payload(
    arg: &str,
    mut stdin: impl tokio::io::AsyncRead + Unpin,
) -> std::io::Result<Vec<u8>> {
    if arg == "-" {
        let mut bytes = Vec::new();
        stdin.read_to_end(&mut bytes).await?;
        Ok(bytes)
    } else {
        Ok(arg.as_bytes().to_vec())
    }
}

/// Sends a `Subscribe` for every filter in `filters`, waits until each
/// has been acked, then prints every delivered message (per `output`,
/// see [`OutputMode`]) until interrupted (Ctrl-C).
async fn subscribe_and_print(
    conn: &mut Compat<MaybeTlsStream>,
    sender: PeerId,
    filters: Vec<TopicFilter>,
    output: OutputMode,
) -> std::io::Result<()> {
    let backlog = subscribe_all(conn, sender, &filters).await?;
    let list = filters
        .iter()
        .map(TopicFilter::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let banner = format!("Subscribed to {list}. Waiting for messages (Ctrl-C to stop)...");
    // In OutputMode::Raw, stdout is reserved for exactly the delivered
    // payload bytes - this banner (and each message's own note, see
    // print_if_publish) goes to stderr instead there.
    match output {
        OutputMode::Text => println!("{banner}"),
        OutputMode::Raw => eprintln!("{banner}"),
    }
    // Anything subscribe_all had to buffer while still waiting on
    // another filter's ack (see its own doc comment) is real,
    // already-received traffic - print it before falling into the
    // live loop below, in the order it actually arrived.
    for envelope in backlog {
        print_if_publish(envelope, output)?;
    }

    loop {
        tokio::select! {
            envelope = recv(conn) => print_if_publish(envelope?, output)?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

fn print_if_publish(envelope: Envelope, output: OutputMode) -> std::io::Result<()> {
    let MessageKind::Publish { topic, payload } = envelope.kind else {
        return Ok(());
    };
    match output {
        OutputMode::Text => println!("[{topic}] {}", String::from_utf8_lossy(&payload)),
        OutputMode::Raw => {
            eprintln!("[{topic}] {} bytes", payload.len());
            let mut stdout = std::io::stdout();
            stdout.write_all(&payload)?;
            stdout.flush()?;
        }
    }
    Ok(())
}

/// Sends a `Subscribe` for `filter` and waits for the matching `Ack` -
/// or, if the node refuses it (e.g. a `--topic-acl`, see ADR-0018, or
/// a wildcard `filter` with one configured, see ADR-0022), the
/// matching `Error`, surfaced as an `Err` rather than waiting forever
/// for an `Ack` that will never come.
///
/// Test-only: `subscribe_and_print`'s actual CLI-facing path goes
/// through [`subscribe_all`] even for a single filter (its per-filter
/// overhead is trivial), so this exists purely so protocol-level tests
/// can drive a single subscribe directly, without simulating Ctrl-C or
/// capturing stdout. See ADR-0033.
#[cfg(test)]
async fn subscribe(
    conn: &mut Compat<MaybeTlsStream>,
    sender: PeerId,
    filter: TopicFilter,
) -> std::io::Result<()> {
    let envelope = Envelope::new(sender, MessageKind::Subscribe { filter });
    send(conn, &envelope).await?;
    loop {
        let received = recv(conn).await?;
        match &received.kind {
            MessageKind::Ack { in_reply_to } if *in_reply_to == envelope.id => return Ok(()),
            MessageKind::Error {
                in_reply_to,
                message,
            } if *in_reply_to == Some(envelope.id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    message.clone(),
                ));
            }
            _ => continue,
        }
    }
}

/// Like [`subscribe`], but for every filter in `filters` at once: sends
/// every `Subscribe` up front, then drains responses until each has
/// been acked (or the first `Error` is hit, which fails the whole
/// batch immediately - not "whichever filters were permitted"). Any
/// `Publish` that arrives before every ack does is returned, in
/// arrival order, rather than acted on here - this function is only
/// about the subscribe handshake, same division of responsibility as
/// [`subscribe`] itself never printing anything.
///
/// Deliberately not just `filters.iter()` calling [`subscribe`] once
/// per filter in sequence: with more than one filter outstanding, an
/// earlier filter can already be acked and start delivering live
/// `Publish` traffic while a later one is still awaiting its own ack -
/// [`subscribe`]'s loop would silently discard that delivery, since it
/// only recognizes the one `Ack`/`Error` it's currently waiting on.
/// This loop instead buffers any `Publish` that arrives in the
/// meantime, alongside tracking every outstanding request by its own
/// `MessageId`. See ADR-0033.
async fn subscribe_all(
    conn: &mut Compat<MaybeTlsStream>,
    sender: PeerId,
    filters: &[TopicFilter],
) -> std::io::Result<Vec<Envelope>> {
    let mut pending: HashSet<MessageId> = HashSet::new();
    for filter in filters {
        let envelope = Envelope::new(
            sender,
            MessageKind::Subscribe {
                filter: filter.clone(),
            },
        );
        send(conn, &envelope).await?;
        pending.insert(envelope.id);
    }
    let mut backlog = Vec::new();
    while !pending.is_empty() {
        let received = recv(conn).await?;
        match &received.kind {
            MessageKind::Ack { in_reply_to } if pending.remove(in_reply_to) => {}
            MessageKind::Error {
                in_reply_to: Some(id),
                message,
            } if pending.remove(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    message.clone(),
                ));
            }
            MessageKind::Publish { .. } => backlog.push(received),
            _ => continue,
        }
    }
    Ok(backlog)
}

/// The connection settings `run()` actually dials with, after merging
/// `Cli`'s flags with the config file (`--config`/ADR-0034) - CLI flag
/// takes precedence, then the config file, then `DEFAULT_ADDR` for
/// `addr` alone (the TLS fields have no built-in default: no
/// `--tls-ca`/`tls_ca` from either source means a plaintext
/// connection, as before this ADR).
#[derive(Debug)]
struct ConnectionOptions {
    addr: String,
    tls_ca: Option<PathBuf>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}

impl ConnectionOptions {
    /// Merges `cli`'s flags over `config`'s fields. clap's `requires`
    /// already enforces `--tls-cert`/`--tls-key` are both-or-neither
    /// for a pure-CLI invocation, but can't see across a CLI flag
    /// paired with the other half from the config file - so that same
    /// constraint is re-checked here, on the merged, effective values.
    fn merge(cli: &Cli, config: config::Config) -> std::io::Result<Self> {
        let tls_cert = cli.tls_cert.clone().or(config.tls_cert);
        let tls_key = cli.tls_key.clone().or(config.tls_key);
        if tls_cert.is_some() != tls_key.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--tls-cert and --tls-key must be given together, whether from flags or the config file (see --config)",
            ));
        }
        Ok(Self {
            addr: cli
                .addr
                .clone()
                .or(config.addr)
                .unwrap_or_else(|| DEFAULT_ADDR.to_owned()),
            tls_ca: cli.tls_ca.clone().or(config.tls_ca),
            tls_cert,
            tls_key,
        })
    }
}

/// Builds this connection's TLS connector from `opts`'s `--tls-*`
/// fields, if `tls_ca` was given (from a flag or the config file) -
/// `None` (plaintext, as before) otherwise. `tls_cert`/`tls_key`, if
/// also given, are presented as this client's own identity;
/// `ConnectionOptions::merge` already enforces they're
/// both-or-neither. See ADR-0016.
fn build_connector(opts: &ConnectionOptions) -> std::io::Result<Option<TlsConnector>> {
    let Some(ca_path) = &opts.tls_ca else {
        return Ok(None);
    };
    let to_io =
        |err: thoth_mesh_tls::TlsError| std::io::Error::new(std::io::ErrorKind::InvalidInput, err);

    let ca = load_certs(ca_path).map_err(to_io)?;
    let identity = match (&opts.tls_cert, &opts.tls_key) {
        (Some(cert_path), Some(key_path)) => Some((
            load_certs(cert_path).map_err(to_io)?,
            load_private_key(key_path).map_err(to_io)?,
        )),
        _ => None,
    };
    let config = client_config(ca, identity).map_err(to_io)?;
    Ok(Some(TlsConnector::from(std::sync::Arc::new(config))))
}

fn parse_topic(s: &str) -> std::io::Result<Topic> {
    s.parse().map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid topic {s:?}: {err}"),
        )
    })
}

fn parse_filter(s: &str) -> std::io::Result<TopicFilter> {
    s.parse().map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid topic filter {s:?}: {err}"),
        )
    })
}

async fn send(conn: &mut Compat<MaybeTlsStream>, envelope: &Envelope) -> std::io::Result<()> {
    let bytes = envelope
        .to_bytes()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    async_framing::write_frame(conn, &bytes)
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))
}

async fn recv(conn: &mut Compat<MaybeTlsStream>) -> std::io::Result<Envelope> {
    let bytes = async_framing::read_frame(conn)
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    Envelope::from_bytes(&bytes).map_err(|err| std::io::Error::other(err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use thoth_mesh_core::PeerId;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    async fn spawn_test_node() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(thoth_mesh_node::serve(listener, Vec::new()));
        addr
    }

    async fn connect(addr: std::net::SocketAddr) -> Compat<MaybeTlsStream> {
        let tcp = TcpStream::connect(addr).await.unwrap();
        MaybeTlsStream::Plain(tcp).compat()
    }

    /// A `--config` path guaranteed to not exist, so tests that don't
    /// care about the config file get an empty one deterministically -
    /// not whatever happens to be at the real conventional location on
    /// the machine running the test. See config::load's own tests for
    /// the loading/merging logic itself.
    fn nonexistent_config_path() -> PathBuf {
        PathBuf::from("/nonexistent/thoth-mesh-cli-test/config.toml")
    }

    #[tokio::test]
    async fn run_publish_delivers_to_a_real_subscriber() {
        let addr = spawn_test_node().await;

        // A raw subscriber, playing the role of another client.
        let mut subscriber = connect(addr).await;
        subscribe(
            &mut subscriber,
            PeerId::new(),
            "weather.updates".parse().unwrap(),
        )
        .await
        .unwrap();

        // The CLI's own public entry point, run against the same node.
        let cli = Cli {
            addr: Some(addr.to_string()),
            config: Some(nonexistent_config_path()),
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            command: Command::Publish {
                topic: "weather.updates".into(),
                payload: "sunny".into(),
            },
        };
        run(cli).await.unwrap();

        let delivered = timeout(TEST_TIMEOUT, recv(&mut subscriber))
            .await
            .expect("timed out waiting for the publish")
            .unwrap();
        match delivered.kind {
            MessageKind::Publish { topic, payload } => {
                assert_eq!(topic, "weather.updates".parse().unwrap());
                assert_eq!(payload, b"sunny");
            }
            other => panic!("expected a Publish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_acks_then_receives_a_publish() {
        let addr = spawn_test_node().await;
        let mut client = connect(addr).await;
        let topic: Topic = "weather.updates".parse().unwrap();

        // subscribe() only returns once the ack for our own request
        // arrives - if it returned early or hung, this would fail or
        // time out.
        timeout(
            TEST_TIMEOUT,
            subscribe(&mut client, PeerId::new(), topic.clone().into()),
        )
        .await
        .expect("timed out waiting for the subscribe ack")
        .unwrap();

        let mut publisher = connect(addr).await;
        send(
            &mut publisher,
            &Envelope::new(
                PeerId::new(),
                MessageKind::Publish {
                    topic: topic.clone(),
                    payload: b"jam".to_vec(),
                },
            ),
        )
        .await
        .unwrap();

        let delivered = timeout(TEST_TIMEOUT, recv(&mut client))
            .await
            .expect("timed out waiting for the publish")
            .unwrap();
        assert_eq!(
            delivered.kind,
            MessageKind::Publish {
                topic,
                payload: b"jam".to_vec(),
            }
        );
    }

    #[test]
    fn parse_topic_rejects_invalid_topics() {
        assert!(parse_topic("").is_err());
    }

    #[tokio::test]
    async fn read_payload_uses_the_literal_argument_when_it_is_not_a_dash() {
        // A reader that would panic if actually read from - proves
        // the literal-argument path never touches it.
        let unused = tokio::io::empty();
        let payload = read_payload("hello", unused).await.unwrap();
        assert_eq!(payload, b"hello");
    }

    #[tokio::test]
    async fn read_payload_reads_raw_bytes_from_stdin_when_the_argument_is_a_dash() {
        // Includes bytes that aren't valid UTF-8 - the whole point of
        // reading stdin raw instead of as a CLI-argument String.
        let binary: &[u8] = &[0xff, 0xfe, 0x00, b'h', b'i', 0x01];
        let payload = read_payload("-", binary).await.unwrap();
        assert_eq!(payload, binary);
    }

    #[test]
    fn help_text_has_no_embedded_double_quotes() {
        // A literal `"` in any --help/doc-comment text embeds
        // unescaped into fish's generated `-d`/`-a` arguments and
        // breaks the script at load time - an actual bug this test is
        // here to catch, found by loading a real generated fish
        // script (see ADR-0036), not by reasoning about the generator
        // up front. Checked structurally over the whole Cli command
        // tree, rather than one string at a time, so a doc comment
        // added anywhere later stays covered.
        fn check(cmd: &clap::Command) {
            let texts = [cmd.get_about(), cmd.get_long_about()];
            for text in texts.into_iter().flatten() {
                assert!(
                    !text.to_string().contains('"'),
                    "{}: {text}",
                    cmd.get_name()
                );
            }
            for arg in cmd.get_arguments() {
                let texts = [arg.get_help(), arg.get_long_help()];
                for text in texts.into_iter().flatten() {
                    assert!(
                        !text.to_string().contains('"'),
                        "{}/{}: {text}",
                        cmd.get_name(),
                        arg.get_id()
                    );
                }
                for value in arg.get_possible_values() {
                    if let Some(help) = value.get_help() {
                        assert!(
                            !help.to_string().contains('"'),
                            "{}/{}={}: {help}",
                            cmd.get_name(),
                            arg.get_id(),
                            value.get_name()
                        );
                    }
                }
            }
            for sub in cmd.get_subcommands() {
                check(sub);
            }
        }
        check(&Cli::command());
    }

    #[test]
    fn print_completions_registers_under_the_real_binary_name_for_every_shell() {
        for shell in clap_complete::Shell::value_variants() {
            let mut out = Vec::new();
            print_completions(*shell, &mut out).unwrap();
            let script = String::from_utf8(out).unwrap();
            assert!(
                !script.is_empty(),
                "{shell:?} produced an empty completion script"
            );
            // Regression check for the name mismatch this depended on
            // fixing (see ADR-0036): the crate is thoth-mesh-cli, but
            // that's not the binary anyone actually runs.
            assert!(
                !script.contains("thoth-mesh-cli") && !script.contains("thoth__mesh__cli"),
                "{shell:?} completions registered under the crate name, not the thoth-mesh binary:\n{script}"
            );
        }
    }

    #[tokio::test]
    async fn run_completions_short_circuits_before_touching_the_network() {
        // Nothing listens on this address - if run() tried to connect
        // before handling Completions, this would time out or fail
        // with a connection error instead of succeeding.
        let cli = Cli {
            addr: Some("127.0.0.1:1".to_owned()),
            config: Some(nonexistent_config_path()),
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            command: Command::Completions {
                shell: clap_complete::Shell::Bash,
            },
        };
        timeout(TEST_TIMEOUT, run(cli))
            .await
            .expect("run() should handle Completions before ever touching the network")
            .unwrap();
    }

    #[tokio::test]
    async fn run_rejects_an_invalid_filter_before_connecting() {
        // Nothing listens on this address - if run() tried to connect
        // before validating its argument, this would time out or fail
        // with a connection error instead of InvalidInput.
        let cli = Cli {
            addr: Some("127.0.0.1:1".to_owned()),
            config: Some(nonexistent_config_path()),
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            command: Command::Subscribe {
                filters: vec!["weather.#.updates".to_owned()],
                output: OutputMode::Text,
            },
        };
        let err = timeout(TEST_TIMEOUT, run(cli))
            .await
            .expect("run() should fail validation before ever touching the network")
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn parse_filter_rejects_invalid_filters() {
        assert!(parse_filter("").is_err());
        assert!(parse_filter("weather.#.updates").is_err());
    }

    #[test]
    fn parse_filter_accepts_a_wildcard_pattern() {
        assert!(parse_filter("weather.+").is_ok());
        assert!(parse_filter("weather.#").is_ok());
    }

    #[tokio::test]
    async fn subscribe_with_a_wildcard_filter_receives_a_matching_publish() {
        // The CLI's own Command::Subscribe now accepts ADR-0022's
        // wildcard syntax, not just a literal topic name.
        let addr = spawn_test_node().await;
        let mut client = connect(addr).await;
        let filter: TopicFilter = "weather.+".parse().unwrap();

        timeout(TEST_TIMEOUT, subscribe(&mut client, PeerId::new(), filter))
            .await
            .expect("timed out waiting for the subscribe ack")
            .unwrap();

        let mut publisher = connect(addr).await;
        let topic: Topic = "weather.updates".parse().unwrap();
        send(
            &mut publisher,
            &Envelope::new(
                PeerId::new(),
                MessageKind::Publish {
                    topic: topic.clone(),
                    payload: b"sunny".to_vec(),
                },
            ),
        )
        .await
        .unwrap();

        let delivered = timeout(TEST_TIMEOUT, recv(&mut client))
            .await
            .expect("timed out waiting for the publish")
            .unwrap();
        assert_eq!(
            delivered.kind,
            MessageKind::Publish {
                topic,
                payload: b"sunny".to_vec(),
            }
        );
    }

    #[tokio::test]
    async fn subscribe_returns_an_error_when_the_topic_acl_rejects_it() {
        // A node with a topic ACL that grants nothing at all for
        // "secret.topic" - every Subscribe to it is refused (ADR-0018).
        let acl = thoth_mesh_node::TopicAcl::parse(["anonymous|sub|weather.updates"]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(thoth_mesh_node::serve_with_tls(
            listener,
            Vec::new(),
            thoth_mesh_node::NodeOptions {
                topic_acl: Some(acl),
                ..Default::default()
            },
        ));

        let mut client = connect(addr).await;
        let err = timeout(
            TEST_TIMEOUT,
            subscribe(&mut client, PeerId::new(), "secret.topic".parse().unwrap()),
        )
        .await
        .expect("timed out waiting for the rejection - subscribe() must not hang on an Error")
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn subscribe_all_acks_every_filter_and_each_still_delivers() {
        let addr = spawn_test_node().await;
        let mut client = connect(addr).await;
        let weather: TopicFilter = "weather.updates".parse().unwrap();
        let traffic: TopicFilter = "traffic.updates".parse().unwrap();

        let backlog = timeout(
            TEST_TIMEOUT,
            subscribe_all(
                &mut client,
                PeerId::new(),
                &[weather.clone(), traffic.clone()],
            ),
        )
        .await
        .expect("timed out waiting for both acks")
        .unwrap();
        assert!(
            backlog.is_empty(),
            "nothing was published yet, so there's nothing to have buffered"
        );

        let mut publisher = connect(addr).await;
        for (topic, payload) in [
            ("weather.updates", b"sunny".to_vec()),
            ("traffic.updates", b"jammed".to_vec()),
        ] {
            send(
                &mut publisher,
                &Envelope::new(
                    PeerId::new(),
                    MessageKind::Publish {
                        topic: topic.parse().unwrap(),
                        payload,
                    },
                ),
            )
            .await
            .unwrap();
        }

        let mut delivered = Vec::new();
        for _ in 0..2 {
            let envelope = timeout(TEST_TIMEOUT, recv(&mut client))
                .await
                .expect("timed out waiting for a publish")
                .unwrap();
            if let MessageKind::Publish { topic, payload } = envelope.kind {
                delivered.push((topic.to_string(), payload));
            }
        }
        delivered.sort();
        assert_eq!(
            delivered,
            vec![
                ("traffic.updates".to_owned(), b"jammed".to_vec()),
                ("weather.updates".to_owned(), b"sunny".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn subscribe_all_buffers_a_publish_that_arrives_before_every_ack_does() {
        // A raw socket standing in for a node - not spawn_test_node(),
        // deliberately: this test needs to choose exactly which order
        // responses arrive in (a live publish for the first filter,
        // interleaved *ahead of* the second filter's own ack), not
        // hope real server/forwarder scheduling happens to produce
        // that interleaving on a given run.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fake_node = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut conn = MaybeTlsStream::Plain(socket).compat();

            // The client sends filters in order, awaiting each send()
            // before the next - so the first frame read here is
            // reliably weather's Subscribe, the second traffic's.
            let weather_subscribe = recv(&mut conn).await.unwrap();
            let traffic_subscribe = recv(&mut conn).await.unwrap();

            send(
                &mut conn,
                &Envelope::new(
                    PeerId::new(),
                    MessageKind::Ack {
                        in_reply_to: weather_subscribe.id,
                    },
                ),
            )
            .await
            .unwrap();
            send(
                &mut conn,
                &Envelope::new(
                    PeerId::new(),
                    MessageKind::Publish {
                        topic: "weather.updates".parse().unwrap(),
                        payload: b"sunny".to_vec(),
                    },
                ),
            )
            .await
            .unwrap();
            send(
                &mut conn,
                &Envelope::new(
                    PeerId::new(),
                    MessageKind::Ack {
                        in_reply_to: traffic_subscribe.id,
                    },
                ),
            )
            .await
            .unwrap();
        });

        let mut client = connect(addr).await;
        let weather: TopicFilter = "weather.updates".parse().unwrap();
        let traffic: TopicFilter = "traffic.updates".parse().unwrap();
        let backlog = timeout(
            TEST_TIMEOUT,
            subscribe_all(&mut client, PeerId::new(), &[weather.clone(), traffic]),
        )
        .await
        .expect("timed out waiting for the batch ack")
        .unwrap();
        timeout(TEST_TIMEOUT, fake_node)
            .await
            .expect("fake node task timed out")
            .unwrap();

        // A naive per-filter subscribe() loop would have discarded
        // this - it isn't the ack traffic's Subscribe is waiting on.
        assert_eq!(
            backlog.len(),
            1,
            "the interleaved publish should be buffered, not lost: {backlog:?}"
        );
        assert_eq!(
            backlog[0].kind,
            MessageKind::Publish {
                topic: weather.as_topic().unwrap(),
                payload: b"sunny".to_vec(),
            }
        );
    }

    #[tokio::test]
    async fn subscribe_all_rejects_the_whole_batch_on_the_first_denied_filter() {
        let acl = thoth_mesh_node::TopicAcl::parse(["anonymous|sub|weather.updates"]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(thoth_mesh_node::serve_with_tls(
            listener,
            Vec::new(),
            thoth_mesh_node::NodeOptions {
                topic_acl: Some(acl),
                ..Default::default()
            },
        ));

        let mut client = connect(addr).await;
        let weather: TopicFilter = "weather.updates".parse().unwrap();
        let secret: TopicFilter = "secret.topic".parse().unwrap();
        let err = timeout(
            TEST_TIMEOUT,
            subscribe_all(&mut client, PeerId::new(), &[weather, secret]),
        )
        .await
        .expect("timed out waiting for the rejection")
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    fn publish_cli(addr: Option<String>, config: Option<PathBuf>) -> Cli {
        Cli {
            addr,
            config,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            command: Command::Publish {
                topic: "weather.updates".into(),
                payload: "sunny".into(),
            },
        }
    }

    #[test]
    fn connection_options_merge_prefers_cli_over_config_over_default_addr() {
        let cli = publish_cli(None, None);
        let opts = ConnectionOptions::merge(&cli, config::Config::default()).unwrap();
        assert_eq!(opts.addr, DEFAULT_ADDR);

        let from_config = config::Config {
            addr: Some("127.0.0.2:1".to_owned()),
            ..Default::default()
        };
        let opts = ConnectionOptions::merge(&cli, from_config).unwrap();
        assert_eq!(opts.addr, "127.0.0.2:1");

        let cli_with_addr = publish_cli(Some("127.0.0.3:1".to_owned()), None);
        let from_config = config::Config {
            addr: Some("127.0.0.2:1".to_owned()),
            ..Default::default()
        };
        let opts = ConnectionOptions::merge(&cli_with_addr, from_config).unwrap();
        assert_eq!(
            opts.addr, "127.0.0.3:1",
            "the CLI flag should win over the config file"
        );
    }

    #[test]
    fn connection_options_merge_accepts_a_tls_identity_split_across_cli_and_config() {
        // --tls-cert on the CLI, tls_key only in the config file -
        // clap's own `requires` can't see across sources to allow
        // this, so ConnectionOptions::merge has to.
        let mut cli = publish_cli(None, None);
        cli.tls_cert = Some(PathBuf::from("client.pem"));
        let config = config::Config {
            tls_key: Some(PathBuf::from("client.key")),
            ..Default::default()
        };
        let opts = ConnectionOptions::merge(&cli, config).unwrap();
        assert_eq!(opts.tls_cert, Some(PathBuf::from("client.pem")));
        assert_eq!(opts.tls_key, Some(PathBuf::from("client.key")));
    }

    #[test]
    fn connection_options_merge_rejects_a_tls_cert_with_no_key_from_either_source() {
        // tls_cert set (from the CLI here; the config file would be
        // the same), but tls_key missing from both - unlike the split
        // above, there's no key to pair it with at all.
        let mut cli = publish_cli(None, None);
        cli.tls_cert = Some(PathBuf::from("client.pem"));
        let err = ConnectionOptions::merge(&cli, config::Config::default()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn run_falls_back_to_the_config_files_addr_when_no_cli_flag_is_given() {
        let addr = spawn_test_node().await;

        let mut subscriber = connect(addr).await;
        subscribe(
            &mut subscriber,
            PeerId::new(),
            "weather.updates".parse().unwrap(),
        )
        .await
        .unwrap();

        let dir = std::env::temp_dir().join(format!("thoth-mesh-cli-test-{:?}", PeerId::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.toml");
        std::fs::write(&config_path, format!("addr = \"{addr}\"\n")).unwrap();

        run(publish_cli(None, Some(config_path))).await.unwrap();

        let delivered = timeout(TEST_TIMEOUT, recv(&mut subscriber))
            .await
            .expect("timed out waiting for the publish")
            .unwrap();
        assert_eq!(
            delivered.kind,
            MessageKind::Publish {
                topic: "weather.updates".parse().unwrap(),
                payload: b"sunny".to_vec(),
            }
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn run_prefers_the_cli_addr_flag_over_the_config_files() {
        let addr = spawn_test_node().await;

        let dir =
            std::env::temp_dir().join(format!("thoth-mesh-cli-test-override-{:?}", PeerId::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.toml");
        // A bogus address - if run() used this instead of the --addr
        // flag below, connecting would fail (nothing listens there).
        std::fs::write(&config_path, "addr = \"127.0.0.1:1\"\n").unwrap();

        run(publish_cli(Some(addr.to_string()), Some(config_path)))
            .await
            .unwrap();

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
