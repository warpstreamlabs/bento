---
title: Processing Pipelines
---

Within a Bento configuration, in between `input` and `output`, is a `pipeline` section. This section describes an array of [processors][processors] that are to be applied to *all* messages, and are not bound to any particular input or output.

If you have processors that are heavy on CPU and aren't specific to a certain input or output they are best suited for the pipeline section. It is advantageous to use the pipeline section as it allows you to set an explicit number of parallel threads of execution:

```yaml
input:
  resource: foo

pipeline:
  threads: 4
  processors:
    - mapping: |
        root = this
        fans = fans.map_each(match {
          this.obsession > 0.5 => this
          _ => deleted()
        })

output:
  resource: bar
```

If the field `threads` is set to `-1` (the default) it will automatically match the number of logical CPUs available. By default almost all Bento sources will utilise as many processing threads as have been configured, which makes horizontal scaling easy.

You can also configure your pipeline to run in `strict_mode`, where messages that fail a processing step are rejected and prevented from proceeding to subsequent processors:

```yaml
input:
  resource: foo

pipeline:
  strict_mode: true
  processors:
    - mapping: |
        root = if this.value > 0.5 { throw("error") }
    # If an error is attached to a message, this step is not reached
    - mapping: |
        root.message = this

output:
  resource: bar
```

[processors]: /docs/components/processors/about
