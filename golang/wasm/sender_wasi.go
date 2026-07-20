//go:build wasip1

package wasm

import (
	"context"
	"fmt"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	pluginv1 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"
)

type hostMessageSender struct{ host pluginv1.Host }

type hostKVClient struct{ host pluginv1.Host }

func platformMessageSender() arupa.MessageSender {
	return hostMessageSender{host: pluginv1.NewHost()}
}

func platformKVClient() arupa.KVClient {
	return hostKVClient{host: pluginv1.NewHost()}
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

func (s hostKVClient) KVGet(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	if err := validateKVRequest(namespace, key); err != nil {
		return nil, false, err
	}
	reply, err := s.host.KVGet(ctx, &pluginv1.KVGetRequest{Namespace: namespace, Key: key})
	if err != nil {
		return nil, false, fmt.Errorf("arupa/wasm: host kv get: %w", err)
	}
	return append([]byte(nil), reply.GetValue()...), reply.GetFound(), nil
}

func (s hostKVClient) KVSet(ctx context.Context, namespace, key string, value []byte) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	reply, err := s.host.KVSet(ctx, &pluginv1.KVSetRequest{Namespace: namespace, Key: key, Value: append([]byte(nil), value...)})
	if err != nil {
		return fmt.Errorf("arupa/wasm: host kv set: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return fmt.Errorf("arupa/wasm: host kv set: %s", message)
	}
	return nil
}

func (s hostKVClient) KVDelete(ctx context.Context, namespace, key string) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	reply, err := s.host.KVDelete(ctx, &pluginv1.KVDeleteRequest{Namespace: namespace, Key: key})
	if err != nil {
		return fmt.Errorf("arupa/wasm: host kv delete: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return fmt.Errorf("arupa/wasm: host kv delete: %s", message)
	}
	return nil
}

func (s hostKVClient) KVList(ctx context.Context, namespace string) ([]string, error) {
	if namespace == "" {
		return nil, fmt.Errorf("arupa: kv namespace is required")
	}
	reply, err := s.host.KVList(ctx, &pluginv1.KVListRequest{Namespace: namespace})
	if err != nil {
		return nil, fmt.Errorf("arupa/wasm: host kv list: %w", err)
	}
	return append([]string(nil), reply.GetKeys()...), nil
}

func validateKVRequest(namespace, key string) error {
	if namespace == "" {
		return fmt.Errorf("arupa: kv namespace is required")
	}
	if key == "" {
		return fmt.Errorf("arupa: kv key is required")
	}
	return nil
}
