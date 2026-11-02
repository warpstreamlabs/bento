fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../../../resources/protos");
    let files = protox::compile(["message.proto", "processor.proto"], ["../../../../resources/protos"])?;
    prost_build::compile_fds(files)?;
    Ok(())
}
