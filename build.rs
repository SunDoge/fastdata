fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["tensorflow/core/example/example.proto"], &["."])?;

    // if let Ok(link_paths) = std::env::var("OPENCV_LINK_PATHS") {
    //     println!("cargo:rustc-env=LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{}", &link_paths);
    // }

    Ok(())
}
