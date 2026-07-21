//go:build !wasip1

package wasm

import (
	"fmt"

	"github.com/SteelDrEgg/arupa-sdk/golang"
)

func platformMessageSender() arupa.MessageSender { return nil }

func platformKVClient() arupa.KVClient { return nil }

func platformParamsClient() paramsClient { return nil }

func platformLogger() arupa.Logger { return nil }

func validateKVRequest(namespace, key string) error {
	if namespace == "" {
		return fmt.Errorf("arupa: kv namespace is required")
	}
	if key == "" {
		return fmt.Errorf("arupa: kv key is required")
	}
	return nil
}
