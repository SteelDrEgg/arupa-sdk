//go:build wasip1

package wasm

import servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"

func platformHostClient() servicev2.Host {
	return servicev2.NewHost()
}
