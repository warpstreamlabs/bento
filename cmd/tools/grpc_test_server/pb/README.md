Generate Go bindings (requires protoc and protoc-gen-go, protoc-gen-go-grpc):

protoc --go_out=. --go-grpc_out=. chat.proto


