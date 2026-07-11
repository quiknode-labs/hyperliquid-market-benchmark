fn main() {
    let source_commit = std::env::var("BENCHMARK_SOURCE_COMMIT")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned());
    if source_commit != "unavailable"
        && (!(7..=64).contains(&source_commit.len())
            || !source_commit.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        panic!("BENCHMARK_SOURCE_COMMIT must be a 7-64 character hexadecimal Git commit");
    }
    println!(
        "cargo:rustc-env=BENCHMARK_SOURCE_COMMIT={}",
        source_commit.to_ascii_lowercase()
    );
    println!("cargo:rerun-if-env-changed=BENCHMARK_SOURCE_COMMIT");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);

    tonic_prost_build::configure()
        .build_server(false)
        .compile_with_config(config, &["proto/orderbook.proto"], &["proto"])
        .expect("compile public order-book client protocol");

    println!("cargo:rerun-if-changed=proto/orderbook.proto");
}
