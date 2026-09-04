//! Config file support for connection options (`--addr`, `--tls-*`) -
//! see ADR-0034. Kept out of `lib.rs` since parsing/locating a config
//! file is a distinct concern from driving the wire protocol.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The subset of `Cli`'s flags that a config file can supply a default
/// for. Every field is optional - an empty file is a valid, no-op
/// config, same as no file at all.
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub addr: Option<String>,
    pub tls_ca: Option<PathBuf>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
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

/// The conventional per-OS config file path: `config.toml` under
/// `directories::ProjectDirs`' config directory for `thoth-mesh`
/// (`~/.config/thoth-mesh/config.toml` on Linux, and the
/// platform-appropriate equivalent on macOS/Windows).
fn default_path() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "", "thoth-mesh")
        .map(|dirs| dirs.config_dir().join("config.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonexistent_path() -> PathBuf {
        // Fixed, deliberately-bogus path - never created by any test,
        // so this is deterministic without a tempfile dependency.
        PathBuf::from("/nonexistent/thoth-mesh-config-file-test/config.toml")
    }

    #[test]
    fn load_with_a_missing_explicit_path_returns_an_empty_config() {
        assert_eq!(load(Some(&nonexistent_path())).unwrap(), Config::default());
    }

    #[test]
    fn load_parses_every_field_from_a_real_file() {
        let dir =
            std::env::temp_dir().join(format!("thoth-mesh-config-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
                addr = "127.0.0.2:49500"
                tls_ca = "/etc/thoth-mesh/ca.pem"
                tls_cert = "/etc/thoth-mesh/client.pem"
                tls_key = "/etc/thoth-mesh/client.key"
            "#,
        )
        .unwrap();

        let config = load(Some(&path)).unwrap();
        assert_eq!(
            config,
            Config {
                addr: Some("127.0.0.2:49500".to_owned()),
                tls_ca: Some(PathBuf::from("/etc/thoth-mesh/ca.pem")),
                tls_cert: Some(PathBuf::from("/etc/thoth-mesh/client.pem")),
                tls_key: Some(PathBuf::from("/etc/thoth-mesh/client.key")),
            }
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_rejects_an_unrecognized_key() {
        let dir = std::env::temp_dir().join(format!(
            "thoth-mesh-config-test-badkey-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "adress = \"127.0.0.1:1\"\n").unwrap();

        let err = load(Some(&path)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn default_path_points_at_a_thoth_mesh_config_toml() {
        // Not exercising load(None) itself - that would depend on
        // whatever's actually on the machine running this test - just
        // that the conventional location resolves to the expected
        // file name under a directory named for this project.
        let Some(path) = default_path() else {
            // No resolvable home directory in this environment - not
            // this function's bug to fail over.
            return;
        };
        assert_eq!(path.file_name().unwrap(), "config.toml");
        assert!(path.to_string_lossy().contains("thoth-mesh"));
    }
}
