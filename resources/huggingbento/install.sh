#!/bin/bash

ONNXRUNTIME_VERSION=${ONNXRUNTIME_VERSION:-"1.18.0"} 
DEPENDENCY_DEST=${DEPENDENCY_DEST:-"/usr/lib"} 

# Get the OS and ARCH in correct format to download ONNX libs
# Note: only linux and mac supported

# Get OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS="osx"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="linux"
else
    echo "Unsupported OS"
    exit 1
fi

# Get architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64|amd64)
        ARCH="x64"
        ;;
    aarch64|arm64)
        ARCH="arm64"
        ;;
    i386|i686)
        ARCH="x86"
        ;;
    *)
        echo "Unsupported architecture"
        exit 1
        ;;
esac

# Special case for macOS universal binary
if [[ "$OS" == "osx" && "$ARCH" == "x64" ]]; then
    FILENAME="onnxruntime-osx-universal2-1.19.0.tgz"
else
    FILENAME="onnxruntime-$OS-$ARCH-1.19.0.tgz"
fi

# For Windows, use .zip instead of .tgz
if [[ "$OS" == "win" ]]; then
    FILENAME="${FILENAME%.tgz}.zip"
fi

echo "Detected OS: $OS"
echo "Detected architecture: $ARCH"

# Download ONNX
echo "Downloading v${ONNXRUNTIME_VERSION}/onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}.tgz..." 
curl -LOs https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}/onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}.tgz
echo "Completed downloading v${ONNXRUNTIME_VERSION}/onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}.tgz" 

tar -xzf onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}.tgz

source_file_prefix="./onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}/lib/libonnxruntime.${ONNXRUNTIME_VERSION}"
target_file="${DEPENDENCY_DEST}/onnxruntime.so"

if [[ -f "${source_file_prefix}.so" ]]; then
    mv -f "${source_file_prefix}.so" "${target_file}"
elif [[ -f "${source_file_prefix}.dylib" ]]; then
    mv -f ${source_file_prefix}.dylib "${target_file}"
else
    echo "Error: Neither .so nor .dylib file found."
    exit 1
fi
rm onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}.tgz

# Get tokenizer library

GOOS=$(go env GOOS)
GOARCH=$(go env GOARCH)

tokenizer_version=$(go list -m -f '{{.Version}}' 'github.com/daulet/tokenizers')
tokenizer_version=$(echo $tokenizer_version | awk -F'-' '{print $NF}')
echo "Downloading ${tokenizer_version}/libtokenizers.${GOOS}-${GOARCH}.tar.gz..." 
curl -LOs https://github.com/daulet/tokenizers/releases/download/${tokenizer_version}/libtokenizers.${GOOS}-${GOARCH}.tar.gz
echo "Completed downloading ${tokenizer_version}/libtokenizers.${GOOS}-${GOARCH}.tar.gz." 

tar -C ${DEPENDENCY_DEST} -xzf libtokenizers.${GOOS}-${GOARCH}.tar.gz
rm libtokenizers.${GOOS}-${GOARCH}.tar.gz

