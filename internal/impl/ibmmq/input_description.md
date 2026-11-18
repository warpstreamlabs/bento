This input consumes messages from an IBM MQ queue and forwards only the message payload content downstream.

Each message is read under syncpoint and committed once Bento acknowledges it. If Bento rejects the message, or the input disconnects before it is acknowledged, the message is backed out and stays on the queue.

The next message is read only once the previous one is acknowledged, so messages from this input cannot be batched at the output level.

By default Bento does not build with components that require linking to external libraries. If you wish to build Bento locally with this component then install the IBM MQ C client in `/opt/mqm` and set the build tag `x_bento_extra`:

```shell
# With go
go install -tags "x_bento_extra" github.com/warpstreamlabs/bento/cmd/bento@latest

# Using make
make TAGS=x_bento_extra
```

The resulting binary also needs the IBM MQ client libraries in `/opt/mqm` wherever it runs.

There is a specific docker tag postfix `-cgo` for C builds containing this component.
