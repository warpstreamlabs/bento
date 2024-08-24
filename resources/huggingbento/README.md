# HuggingBento

HuggingBento is a distribution of Bento with used used for running Hugging Face transformer pipelines. Built on [Knight's Analytics Hugot](https://github.com/knights-analytics/hugot) library which has two external dependencies:
- A Open Neural Network Exchange (ONNX) Runtime dynamic library ([See installation](#install-onnx-runtime))
- A Hugging Face tokenizers C binding ([See installation](#install-the-hugging-face-tokenizers-binding))

For the purposes of getting started, we'd suggest instead [running with Docker](#run-with-docker), where the `warpstreamlabs/huggingbento` image has the necessary dependencies baked in.

## Running inference

HuggingBento can either be passed a locally downloaded model:
```yaml
pipeline:
  processors:
    - huggingface_resource:
        pipeline_name: classify-with-local-model
        model_path: "./models/KnightsAnalytics_distilbert-base-uncased-finetuned-sst-2-english"
```

Otherwise, you can toggle `enable_model_download: true` and set the `model_repository` to a HuggingFace repository one or more valid ONNX models:
```yaml
pipeline:
  processors:
    - huggingface_resource:
        pipeline_name: classify-with-downloaded-model
        model_path: "./models"
        enable_model_download: true
          model_repository: "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english"
```

## Run with Docker

```shell
docker run --rm -v /path/to/config.yaml:/bento.yaml -v /path/to/model/repository:/model_repository warpstreamlabs/huggingbento:latest
```

## Install ONNX Runtime
You can follow the [official getting started instructions](https://onnxruntime.ai/getting-started) or follow the guide below:

Find the appropriate prebuilt Open Neural Network Exchange (ONNX) Runtime libraries at the [microsoft/onnxruntime](https://github.com/microsoft/onnxruntime/releases) release page. While, HuggingBento is currently built and tested using `v1.18.x`, feel free to experiment with different versions.

### ONNX Runtime Supported Platforms

ONNX Runtime version +`1.18` supports the following operating system and architectures combintations:

| Operating System (OS) | Architecture (ARCH)| CPU | GPU |
|-----------------------|--------------------|-----|-----|
| `linux`               | `x64`              | ✅   | ✅  |
| `linux`               | `arm64`            | ✅   | ❌  |
| `osx`                 | `x86_64`           | ✅   | ❌  |
| `osx`                 | `arm64`            | ✅   | ❌  |
| `osx`                 | `universal2`       | ✅   | ❌  |
| `windows`             | `x64`              | ✅   | ✅  |
| `windows`             | `x86`              | ✅   | ❌  |
| `windows`             | `arm64`            | ✅   | ❌  |


## Notes:
- HuggingBento has not been CUDA or GPU tested but the underlying `Hugot` library has -- so proceed with caution!
- GPU support is available for Linux and Windows x64 builds with CUDA.
- Training support is available for all Windows architectures and Linux x64 and ARM64.
- macOS builds currently do not support GPU acceleration or training.

For the latest information and downloads, please visit the [official ONNX Runtime GitHub repository](https://github.com/microsoft/onnxruntime).

```shell
curl -LO https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}/onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}.tgz && tar -xzf onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}.tgz
```

### For Linux
```shell
mv ./onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}/lib/libonnxruntime.${ONNXRUNTIME_VERSION}.so /usr/lib/onnxruntime.so
```

### For Mac

Using shell:
```shell
mv ./onnxruntime-${OS}-${ARCH}-${ONNXRUNTIME_VERSION}/lib/libonnxruntime.${ONNXRUNTIME_VERSION}.dylib /usr/local/lib/onnxruntime.so
```

Using Homebrew:
```shell
brew install onnxruntime && mv /opt/homebrew/opt/onnxruntime/lib/libonnxruntime.dylib /usr/local/lib/onnxruntime.so
```

## Install the Hugging Face tokenizers binding

Hugot uses Hugging Face tokenizers that are compiled from Rust, meaning you'll also need the `libtokenizers.a`.

These can either be [compiled yourself](https://github.com/daulet/tokenizers?tab=readme-ov-file#installation) or downloaded as a [release](https://github.com/daulet/tokenizers).

You can use the below (flaky) script to download the correct C bindings matching the current version on `github.com/daulet/tokenizers`:
```shell
GOOS=$(go env GOOS)
GOARCH=$(go env GOARCH)

tokenizer_version=$(go list -m -f '{{.Version}}' 'github.com/daulet/tokenizers')
tokenizer_version=$(echo $tokenizer_version | awk -F'-' '{print $NF}')
echo "Downloading ${tokenizer_version}/libtokenizers.${GOOS}-${GOARCH}.tar.gz..." 
curl -LOs https://github.com/daulet/tokenizers/releases/download/${tokenizer_version}/libtokenizers.${GOOS}-${GOARCH}.tar.gz
echo "Completed downloading ${tokenizer_version}/libtokenizers.${GOOS}-${GOARCH}.tar.gz." 

tar -C ${DEPENDENCY_DEST} -xzf libtokenizers.${GOOS}-${GOARCH}.tar.gz
rm libtokenizers.${GOOS}-${GOARCH}.tar.gz
```
