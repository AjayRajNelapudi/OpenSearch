fn main() {
    prost_build::compile_protos(&["proto/datafusion_stats.proto"], &["proto/"]).unwrap();
}
