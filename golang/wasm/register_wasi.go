//go:build wasip1

package wasm

import pluginv1 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"

// RegisterPlugin exports plugin through the WASM ABI.
func RegisterPlugin(plugin pluginv1.Plugin) {
	pluginv1.RegisterPlugin(plugin)
}
