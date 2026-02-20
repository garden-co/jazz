fn main() {
    println!("cargo:rerun-if-changed=../../specs/bench/server_event.capnp");

    capnpc::CompilerCommand::new()
        .src_prefix("../../specs/bench")
        .file("../../specs/bench/server_event.capnp")
        .run()
        .expect("failed to compile capnp benchmark schema");
}
