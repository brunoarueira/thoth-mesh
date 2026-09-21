//! Config file support for every daemon flag - see ADR-0054. Kept out
//! of `main.rs` since parsing/locating a config file is a distinct
//! concern from driving the CLI/daemon itself. Structurally mirrors
//! `thoth-mesh-cli`'s own `config` module (ADR-0034) - same
//! load/locate mechanism, a deliberately separate `Config` type (see
//! ADR-0054's "no code sharing" decision).

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The full set of `Cli` flags a config file can supply a default
/// for - every flag `thoth-mesh-node` takes, not a subset: unlike the
/// CLI (ADR-0034), the daemon has no per-invocation flag surface at
/// all, so every flag is exactly the kind of rarely-changing setting
/// this file exists for. Every field is optional - an empty file is a
/// valid, no-op config, same as no file at all.
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub addr: Option<String>,
    pub log_level: Option<String>,
    #[serde(default)]
    pub peer: Vec<String>,
    pub metrics_addr: Option<String>,
    pub data_dir: Option<PathBuf>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_ca: Option<PathBuf>,
    #[serde(default)]
    pub allow_peer: Vec<String>,
    #[serde(default)]
    pub topic_acl: Vec<String>,
    #[serde(default)]
    pub peer_topic_acl: Vec<String>,
    #[serde(default)]
    pub peer_topic_filter: Vec<String>,
    pub metrics_token_file: Option<PathBuf>,
    pub persisted_message_ttl_secs: Option<u64>,
    pub dead_letter_topic: Option<String>,
    pub publish_rate_limit_per_sec: Option<u32>,
    pub publish_rate_limit_burst: Option<u32>,
}

/// Loads the config file at `explicit` (from `--config`), or, if not
/// given, at the conventional per-OS location. A file that doesn't
/// exist - at either kind of path - is not an error, and yields the
/// same empty `Config` as never having one at all; a file that exists
/// but fails to parse is.
pub fn load(explicit: Option<&Path>) -> std::io::Result<Config> {
    let path = match explicit {
        Some(path) => path.to_path_buf(),
        None => match default_path() {
            Some(path) => path,
            // No resolvable home directory to look under (rare) -
            // nothing to load, same as a missing file.
            None => return Ok(Config::default()),
        },
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => toml::from_str(&contents).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("parsing config file {}: {err}", path.display()),
            )
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(err) => Err(err),
    }
}

/// The conventional per-OS config file path: `node.toml` under
/// `directories::ProjectDirs`' config directory for `thoth-mesh`
/// (`~/.config/thoth-mesh/node.toml` on Linux, and the platform-
/// appropriate equivalent on macOS/Windows) - the same directory
/// `thoth-mesh-cli`'s own `config.toml` lives in (ADR-0034), since
/// both binaries belong to one project, just a different file.
fn default_path() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "", "thoth-mesh")
        .map(|dirs| dirs.config_dir().join("node.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonexistent_path() -> PathBuf {
        // Fixed, deliberately-bogus path - never created by any test,
        // so this is deterministic without a tempfile dependency.
        PathBuf::from("/nonexistent/thoth-mesh-node-config-file-test/node.toml")
    }

    #[test]
    fn load_with_a_missing_explicit_path_returns_an_empty_config() {
        assert_eq!(load(Some(&nonexistent_path())).unwrap(), Config::default());
    }

    #[test]
    fn load_parses_every_field_from_a_real_file() {
        let dir = std::env::temp_dir().join(format!(
            "thoth-mesh-node-config-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.toml");
        std::fs::write(
            &path,
            r#"
                addr = "0.0.0.0:49500"
                log_level = "debug"
                peer = ["node-b:49500", "node-c:49500"]
                metrics_addr = "0.0.0.0:9090"
                data_dir = "/var/lib/thoth-mesh"
                tls_cert = "/etc/thoth-mesh/node-cert.pem"
                tls_key = "/etc/thoth-mesh/node-key.pem"
                tls_ca = "/etc/thoth-mesh/ca-cert.pem"
                allow_peer = ["3F:08:CA:D2"]
                topic_acl = ["anonymous|sub|status.public"]
                peer_topic_acl = ["anonymous|pub|weather.updates"]
                peer_topic_filter = ["3F:08:CA:D2|weather.updates"]
                metrics_token_file = "/etc/thoth-mesh/metrics-token"
                persisted_message_ttl_secs = 604800
                dead_letter_topic = "dead-letter"
                publish_rate_limit_per_sec = 100
                publish_rate_limit_burst = 200
            "#,
        )
        .unwrap();

        let config = load(Some(&path)).unwrap();
        assert_eq!(
            config,
            Config {
                addr: Some("0.0.0.0:49500".to_owned()),
                log_level: Some("debug".to_owned()),
                peer: vec!["node-b:49500".to_owned(), "node-c:49500".to_owned()],
                metrics_addr: Some("0.0.0.0:9090".to_owned()),
                data_dir: Some(PathBuf::from("/var/lib/thoth-mesh")),
                tls_cert: Some(PathBuf::from("/etc/thoth-mesh/node-cert.pem")),
                tls_key: Some(PathBuf::from("/etc/thoth-mesh/node-key.pem")),
                tls_ca: Some(PathBuf::from("/etc/thoth-mesh/ca-cert.pem")),
                allow_peer: vec!["3F:08:CA:D2".to_owned()],
                topic_acl: vec!["anonymous|sub|status.public".to_owned()],
                peer_topic_acl: vec!["anonymous|pub|weather.updates".to_owned()],
                peer_topic_filter: vec!["3F:08:CA:D2|weather.updates".to_owned()],
                metrics_token_file: Some(PathBuf::from("/etc/thoth-mesh/metrics-token")),
                persisted_message_ttl_secs: Some(604800),
                dead_letter_topic: Some("dead-letter".to_owned()),
                publish_rate_limit_per_sec: Some(100),
                publish_rate_limit_burst: Some(200),
            }
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_with_an_empty_file_returns_the_default_config() {
        let dir = std::env::temp_dir().join(format!(
            "thoth-mesh-node-config-test-empty-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.toml");
        std::fs::write(&path, "").unwrap();

        assert_eq!(load(Some(&path)).unwrap(), Config::default());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_rejects_an_unrecognized_key() {
        let dir = std::env::temp_dir().join(format!(
            "thoth-mesh-node-config-test-badkey-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.toml");
        std::fs::write(&path, "addres = \"127.0.0.1:1\"\n").unwrap();

        let err = load(Some(&path)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn default_path_points_at_a_node_toml_under_thoth_mesh() {
        // Not exercising load(None) itself - that would depend on
        // whatever's actually on the machine running this test - just
        // that the conventional location resolves to the expected
        // file name under a directory named for this project, the
        // same one thoth-mesh-cli's own config.toml lives in.
        let Some(path) = default_path() else {
            // No resolvable home directory in this environment - not
            // this function's bug to fail over.
            return;
        };
        assert_eq!(path.file_name().unwrap(), "node.toml");
        assert!(path.to_string_lossy().contains("thoth-mesh"));
    }
}
