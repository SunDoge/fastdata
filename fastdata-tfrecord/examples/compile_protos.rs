/// This is not an exmaple, but a tool to generate prost code.

fn main() -> std::io::Result<()> {
    std::env::set_var("OUT_DIR", "src/proto");
    prost_build::compile_protos(&["tensorflow/core/example/example.proto"], &["."])?;
    Ok(())
}
