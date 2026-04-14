use std::env;
use std::path::PathBuf;
use libbpf_cargo::SkeletonBuilder;

fn main() {
    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("ipblower.skel.rs");

    // Compile C to BPF Object and generate Rust Skeleton
    SkeletonBuilder::new()
        .source("src/bpf/ipblower.bpf.c")
        .build_and_generate(&out)
        .unwrap();

    // Rebuild if the C code changes
    println!("cargo:rerun-if-changed=src/bpf/ipblower.bpf.c");
}
