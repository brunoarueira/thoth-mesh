//! ADR-0056: proves the real wiring, not just the `Readiness` type in
//! isolation (already covered by `metrics_server.rs`'s own unit
//! tests, which construct a `Readiness` by hand) - that `spawn_with_tls`
//! actually marks a node's `Readiness` (exposed via `Node::readiness`)
//! at the right moment for each of its two cases: immediately with no
//! `--data-dir` to rehydrate from, or only once that rehydration -
//! which runs in a background task `spawn_with_tls` itself doesn't
//! wait for - actually finishes.

use thoth_mesh_node::NodeOptions;
use tokio::net::TcpListener;

use thoth_mesh_node::test_support::eventually;

#[tokio::test]
async fn a_spawned_node_with_no_data_dir_is_ready_immediately() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = thoth_mesh_node::spawn(listener, Vec::new());

    // Nothing to rehydrate - `spawn_with_tls` marks this
    // synchronously, before it even returns, not from a background
    // task `accept_loop` would otherwise have to wait on.
    assert!(node.readiness.is_ready());
}

#[tokio::test(flavor = "current_thread")]
async fn a_spawned_node_with_a_data_dir_becomes_ready_only_once_rehydrated() {
    let data_dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = thoth_mesh_node::spawn_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            data_dir: Some(data_dir.path().to_path_buf()),
            ..Default::default()
        },
    )
    .unwrap();

    // Rehydration runs in a background task `spawn_with_tls` itself
    // never awaits (it isn't `async`) - this assertion is still
    // deterministic, not racy: `#[tokio::test]` defaults to a
    // current-thread runtime, so that task never actually runs until
    // this test function itself yields at an `.await`, which hasn't
    // happened yet at this point.
    assert!(
        !node.readiness.is_ready(),
        "shouldn't be ready before the background rehydration task has had a chance to run at all"
    );

    eventually(|| node.readiness.is_ready()).await;
}
