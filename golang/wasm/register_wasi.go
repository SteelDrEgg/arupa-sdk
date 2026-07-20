//go:build wasip1

package wasm

import pluginv1 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"

// Register exports plugin through the WASM ABI.
func Register(plugin *HTTPPlugin) {
	pluginv1.RegisterPlugin(plugin)
}
