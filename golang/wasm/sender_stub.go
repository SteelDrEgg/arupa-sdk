//go:build !wasip1

package wasm

import "github.com/SteelDrEgg/arupa-sdk/golang"

func platformMessageSender() arupa.MessageSender { return nil }
