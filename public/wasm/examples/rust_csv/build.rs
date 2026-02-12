fn main() {
    protobuf_codegen::Codegen::new()
        .pure()
        .cargo_out_dir("protos")
        .include("../../../../resources/protos")
        .input("../../../../resources/protos/message.proto")
        .run_from_script();
}