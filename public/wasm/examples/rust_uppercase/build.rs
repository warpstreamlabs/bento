fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = protox::compile(["message.proto"], ["../../../../resources/protos"])?;
    prost_build::compile_fds(files)?;
    Ok(())
}
