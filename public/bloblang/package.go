// Package bloblang provides high level APIs for registering custom Bloblang
// plugins, as well as for parsing and executing Bloblang mappings.
//
// An example Bento distribution containing component plugins and tests can be found at:
// https://github.com/warpstreamlabs/bento/tree/main/resources/plugin_example
//
// Plugins can either be registered globally, and will be accessible to any
// component parsing Bloblang expressions in the executable, or they can be
// registered as part of an isolated environment.
package bloblang
