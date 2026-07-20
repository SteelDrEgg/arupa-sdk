//go:build wasip1

package wasm

import (
	"context"
	"fmt"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	pluginv1 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"
)

type hostMessageSender struct{ host pluginv1.Host }

func platformMessageSender() arupa.MessageSender {
	return hostMessageSender{host: pluginv1.NewHost()}
}

func (s hostMessageSender) SendMessage(ctx context.Context, message arupa.OutgoingMessage) (string, error) {
	if err := message.Validate(); err != nil {
		return "", err
	}
	reply, err := s.host.SendPluginMessage(ctx, &pluginv1.PluginMessage{
		Target:  message.Target,
		Topic:   message.Topic,
		Payload: append([]byte(nil), message.Payload...),
	})
	if err != nil {
		return "", fmt.Errorf("arupa/wasm: send plugin message: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return "", fmt.Errorf("arupa/wasm: send plugin message: %s", message)
	}
	return reply.GetMessage(), nil
}
