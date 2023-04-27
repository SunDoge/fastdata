fn main() -> std::io::Result<()> {
    prost_build::compile_protos(
        &["tensorflow/core/example/example.proto"],
        &["."],
    )?;
    Ok(())
}
