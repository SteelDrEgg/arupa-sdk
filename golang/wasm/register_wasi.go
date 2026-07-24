//go:build wasip1

package wasm

import servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"

// RegisterService exports service through the WASM ABI.
func RegisterService(service *Service) {
	servicev2.RegisterService(service)
}
