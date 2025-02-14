## Bento Example Plugin Module

This module serves as an example on how to implement components as plugins using the [service package API](https://pkg.go.dev/github.com/warpstreamlabs/bento/public/service), and how to structure a go module to run your own bento distribution. 

```
├── README.md
├── config.yaml             - an example Bento config file using the plugin component from ./plugins/faker.go
├── go.mod                  - Go module definition for this module
├── go.sum                  - Go checksum file
├── plugins                   - Go package 'plugins'
│   ├── faker.go            - An example 'input plugin component'
│   └── faker_test.go       - An example unit test for the plugin
└── main.go                 - Go entrypoint
```

### Tests

The [faker_test.go](./plugins/faker_test.go) contains an example unit test for the `faker` plugin component. You can run all tests with:

```bash
go test ./...
```

### Upgrade Bento Dependency

To update your dependencies, you can use go's built-in [dependency management system](https://go.dev/doc/modules/managing-dependencies) for example:

```bash
go get github.com/warpstreamlabs/bento
```

### Build & Run

Include a [main.go](./main.go) file that has the entrypoint for your bento distribution, and call `service.RunCLI`.
See [main.go](./main.go)'s code comments for options regarding your main.go file.

To build & run a binary of your bento distribution:

```bash
go build
./plugin_example_module -c config.yaml
```

See [config.yaml](./config.yaml) for an example of a bento config using the [faker.go](./plugins/faker.go) plugin.

### More plugin examples

There are a number of examples for different component types available [here](https://pkg.go.dev/github.com/warpstreamlabs/bento/public/service#pkg-examples).