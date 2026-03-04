.PHONY: run venv install lint test clean proto build-wasm

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PROTO_FILE := ../../../../resources/protos/message.proto
PROTO_DIR := $(shell dirname $(PROTO_FILE))
SITE_PACKAGES := $(shell $(PYTHON) -c "import sys; print(sys.path[-1])" 2>/dev/null)

run:
	$(PYTHON) plugin.py

venv:
	python3 -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	curl -Ls https://raw.githubusercontent.com/extism/python-pdk/main/install.sh | PATH="$(PWD)/$(VENV)/bin:$$PATH" bash

proto:
	$(PYTHON) -m grpc_tools.protoc -I$(PROTO_DIR) --python_out=./generated $(PROTO_FILE)

build-wasm: proto
	PYTHONPATH="$(SITE_PACKAGES):." extism-py -o testdata/plugin.wasm plugin.py

test:
	$(PYTHON) -m pytest

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache
	rm -f generated/message_pb2.py testdata/plugin.wasm