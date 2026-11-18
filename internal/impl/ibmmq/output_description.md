This output writes each message payload directly to an IBM MQ queue.

Messages are written one at a time, and each is on the queue once its write succeeds.

By default Bento does not build with components that require linking to external libraries. If you wish to build Bento locally with this component then install the IBM MQ C client in `/opt/mqm` and set the build tag `x_bento_extra`:

```shell
# With go
go install -tags "x_bento_extra" github.com/warpstreamlabs/bento/cmd/bento@latest

# Using make
make TAGS=x_bento_extra
```

The resulting binary also needs the IBM MQ client libraries in `/opt/mqm` wherever it runs.

There is a specific docker tag postfix `-cgo` for C builds containing this component.
