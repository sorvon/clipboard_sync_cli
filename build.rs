fn main() {
    tonic_prost_build::configure()
        .enum_attribute("clipboard.v1.EncodeType", "#[derive(clap::ValueEnum)]")
        .compile_protos(&["proto/clipboard/v1/sync.proto"], &["proto"])
        .unwrap();
}
