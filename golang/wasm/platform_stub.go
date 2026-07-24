//go:build !wasip1

package wasm

import servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"

// The generated WASM Host imports only exist for wasip1. Native builds keep a
// nil implementation so adapters can be unit-tested by injecting hostFactory.
func platformHostClient() servicev2.Host { return nil }
