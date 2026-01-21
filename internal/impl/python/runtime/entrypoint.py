import sys
import struct
import os
import json
import importlib

_SUCCESS_STATUS = 0
_FAILURE_STATUS = 1
_READY_RESPONSE = b"READY"
_EXEC_SCRIPT_LOCATION = "/exec_script.py"

def _load_extra_libs(extra_libs: list[str]):
    injected = {}
    if not extra_libs:
        return injected
    for lib_name in extra_libs:
        name = lib_name.strip()
        if name:
            try:
                injected[name] = importlib.import_module(name)
            except ImportError as e:
                sys.stderr.write(f"Warning: Could not pre-import {name}: {e}\n")
    return injected


def _load_exec_script(path: str):
    with open(path, "r") as f:
        script_source = f.read()

    return compile(script_source, path, 'exec')


def _get_input(payload) -> dict | str | bytearray | bytes:
    if not payload:
        return None
        
    try:
        return json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        try:
            return payload.decode('utf-8')
        except UnicodeDecodeError:
            return payload


def run(compiled_code: str, injected_libs: list[str]):
    sys.stdout.buffer.write(_READY_RESPONSE)
    sys.stdout.buffer.flush()

    while True:
        header = sys.stdin.buffer.read(4)
        if not header: break
        length = struct.unpack(">I", header)[0]
        payload = sys.stdin.buffer.read(length)

        status = _SUCCESS_STATUS
        try:
            input_data = _get_input(payload)
            namespace = {"this": input_data, "root": None, "__builtins__": __builtins__, **injected_libs}
            exec(compiled_code, namespace)
            result_bytes = json.dumps(namespace.get('root')).encode("utf-8")
        except Exception as e:
            status = _FAILURE_STATUS

            import traceback
            print(f"Exception: {type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            
            error_msg = f"{type(e).__name__}: {str(e)}"
            result_bytes = error_msg.encode("utf-8")

        sys.stdout.buffer.write(struct.pack(">BI", status, len(result_bytes)))
        sys.stdout.buffer.write(result_bytes)
        sys.stdout.buffer.flush()

if __name__ == "__main__":
    libs = {}
    if len(sys.argv) > 1:
        libs_str = sys.argv[1] 
        libs = _load_extra_libs(libs_str.split(","))

    try:
        code = _load_exec_script(_EXEC_SCRIPT_LOCATION)
    except Exception as e:
        sys.stderr.write(f"Could not load in and compile script from {_EXEC_SCRIPT_LOCATION}: {e}\n")
        sys.exit(1)

    run(code, libs)