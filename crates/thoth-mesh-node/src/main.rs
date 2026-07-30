//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.

#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!(
        "thoth-mesh-node listening on {}",
        thoth_mesh_node::DEFAULT_ADDR
    );
    thoth_mesh_node::run(thoth_mesh_node::DEFAULT_ADDR).await
}
