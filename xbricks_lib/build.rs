use std::env;
use std::path::PathBuf;
use libbpf_cargo::SkeletonBuilder;

fn main() {
    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("xscrubber.skel.rs");

    // Compile C to BPF Object and generate Rust Skeleton
    SkeletonBuilder::new()
        .source("src/bpf/xscrubber.bpf.c")
        .build_and_generate(&out)
        .unwrap();

    // Rebuild if the BPF C code OR the Wasm Contract changes
    println!("cargo:rerun-if-changed=src/bpf/xscrubber.bpf.c");
    println!("cargo:rerun-if-changed=wit/xbricks.wit");
}
