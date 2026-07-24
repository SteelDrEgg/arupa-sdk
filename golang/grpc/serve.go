package grpc

import (
	"context"
	"fmt"
	"sync"

	servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/grpc"
	hcplugin "github.com/hashicorp/go-plugin"
	googlegrpc "google.golang.org/grpc"
)

const (
	pluginName                = "default_grpc"
	handshakeKey              = "ARUPA_SERVICE"
	handshakeValue            = "arupa-service-v2"
	handshakeVersion          = 2
	grpcMaxReceiveMessageSize = 16 * 1024 * 1024
)

// Serve starts service as an Arupa Service v2 gRPC subprocess. It does not
// return under normal operation.
func Serve(service *Service) {
	ServeServer(service)
}

// ServeServer starts a custom implementation of the generated Service v2
// server. Most services should use Serve and Service instead.
func ServeServer(server servicev2.ServiceServer) {
	hcplugin.Serve(newServeConfig(server))
}

func newServeConfig(server servicev2.ServiceServer) *hcplugin.ServeConfig {
	return &hcplugin.ServeConfig{
		HandshakeConfig: hcplugin.HandshakeConfig{
			ProtocolVersion:  handshakeVersion,
			MagicCookieKey:   handshakeKey,
			MagicCookieValue: handshakeValue,
		},
		Plugins: map[string]hcplugin.Plugin{
			pluginName: &servedPlugin{server: server},
		},
		GRPCServer: newGRPCServer,
	}
}

// newGRPCServer preserves options supplied by go-plugin (including its TLS
// credentials) and raises the receive limit above the host's 8 MiB HTTP body
// limit to leave room for protobuf framing and request metadata.
func newGRPCServer(options []googlegrpc.ServerOption) *googlegrpc.Server {
	serverOptions := make([]googlegrpc.ServerOption, 0, len(options)+1)
	serverOptions = append(serverOptions, options...)
	serverOptions = append(serverOptions, googlegrpc.MaxRecvMsgSize(grpcMaxReceiveMessageSize))
	return googlegrpc.NewServer(serverOptions...)
}

// BrokerReceiver can be implemented by a custom generated ServiceServer to
// receive the go-plugin broker before its Register RPC is served. The server
// can then dial RegisterRequest.host_broker_id and construct a Host client.
//
// Service implements this interface. Custom ServiceServer implementations
// passed to ServeServer should implement it when they need Host callbacks.
type BrokerReceiver interface {
	SetGRPCBroker(*hcplugin.GRPCBroker)
}

type servedPlugin struct {
	hcplugin.NetRPCUnsupportedPlugin

	server servicev2.ServiceServer

	mu     sync.RWMutex
	broker *hcplugin.GRPCBroker
}

func (p *servedPlugin) GRPCServer(broker *hcplugin.GRPCBroker, server *googlegrpc.Server) error {
	if p.server == nil {
		return fmt.Errorf("arupa/grpc: service server is nil")
	}
	if broker == nil {
		return fmt.Errorf("arupa/grpc: gRPC broker is nil")
	}

	// GRPCServer runs before the generated Service can receive Register. Keep
	// the broker on the served plugin and inject it into the SDK wrapper so
	// Register can dial RegisterRequest.host_broker_id.
	p.mu.Lock()
	p.broker = broker
	p.mu.Unlock()
	if receiver, ok := p.server.(BrokerReceiver); ok {
		receiver.SetGRPCBroker(broker)
	}

	servicev2.RegisterServiceServer(server, p.server)
	return nil
}

func (*servedPlugin) GRPCClient(context.Context, *hcplugin.GRPCBroker, *googlegrpc.ClientConn) (any, error) {
	return nil, fmt.Errorf("arupa/grpc: service process does not use GRPCClient")
}

func (p *servedPlugin) currentBroker() *hcplugin.GRPCBroker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.broker
}
