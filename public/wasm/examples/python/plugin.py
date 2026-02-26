import extism
import json
from generated import Batch, Batches

_url = ""
_method = "GET"

@extism.plugin_fn
def init_plugin():
    global _url, _method
    raw = extism.input_bytes()
    if raw:
        conf = json.loads(raw)
        _url = conf.get("url", "")
        _method = conf.get("method", "GET").upper()

@extism.plugin_fn
def process_batch():
    if not _url:
        raise Exception("url is required")

    batch = Batch()
    batch.ParseFromString(extism.input_bytes())

    for part in batch.parts:
        try:
            response = extism.Http.request(_url, meth=_method)

            if response.status_code != 200:
                part.error = f"HTTP {response.status_code}"
                continue

            try:
                part.raw = json.dumps(json.loads(response.data_str())).encode("utf-8")
            except json.JSONDecodeError:
                part.raw = response.data_bytes()

        except Exception as e:
            part.error = str(e)

    out = Batches()
    out.batches.append(batch)
    extism.output_bytes(out.SerializeToString())