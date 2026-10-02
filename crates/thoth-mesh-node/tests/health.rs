//! ADR-0056: proves the real wiring, not just the `Readiness` type in
//! isolation (already covered by `metrics_server.rs`'s own unit
//! tests, which construct a `Readiness` by hand) - that `accept_loop`
//! itself flips a spawned node's readiness once it actually starts,
//! on the exact handle `Node::readiness` exposes.

use tokio::net::TcpListener;

use thoth_mesh_node::test_support::eventually;

#[tokio::test]
async fn a_spawned_nodes_readiness_flips_true_once_it_starts_accepting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = thoth_mesh_node::spawn(listener, Vec::new());

    // `spawn` returns synchronously, having only *scheduled*
    // `accept_loop` as a background task - it hasn't necessarily run
    // yet. This assertion is still deterministic, not racy: `#[tokio::
    // test]` defaults to a current-thread runtime, so a freshly
    // spawned task never actually runs until this test function
    // itself yields at an `.await` - which hasn't happened yet at
    // this point.
    assert!(
        !node.readiness.is_ready(),
        "shouldn't be ready before accept_loop has had a chance to run at all"
    );

    eventually(|| node.readiness.is_ready()).await;
}
