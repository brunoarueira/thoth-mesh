//! Turns `--tls-cert`/`--tls-key`/`--tls-ca` file paths into the
//! `rustls` configs `Shared` needs for both the accept and dial side.
//! See ADR-0016.

use std::path::PathBuf;
use std::sync::Arc;

use thoth_mesh_tls::{
    TlsAcceptor, TlsConnector, client_config, load_certs, load_private_key, server_config,
};

/// Paths to this node's TLS material: its own cert/key, and the CA it
/// trusts (for verifying anyone else's cert). All three are required
/// together to enable TLS at all - see ADR-0016.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

impl TlsConfig {
    /// Builds the accept-side and dial-side `rustls` configs this
    /// node's TLS material describes. The dial side always presents
    /// this node's own identity (a peer dialing a peer always
    /// identifies itself, see ADR-0016) - the same cert/key the
    /// accept side uses.
    pub fn build(&self) -> std::io::Result<(Arc<TlsAcceptor>, Arc<TlsConnector>)> {
        let cert = load_certs(&self.cert).map_err(to_io_error)?;
        let key = load_private_key(&self.key).map_err(to_io_error)?;
        let ca = load_certs(&self.ca).map_err(to_io_error)?;

        let server_cfg =
            server_config(cert.clone(), key.clone_key(), ca.clone()).map_err(to_io_error)?;
        let client_cfg = client_config(ca, Some((cert, key))).map_err(to_io_error)?;

        Ok((
            Arc::new(TlsAcceptor::from(Arc::new(server_cfg))),
            Arc::new(TlsConnector::from(Arc::new(client_cfg))),
        ))
    }
}

fn to_io_error(err: thoth_mesh_tls::TlsError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, err)
}
