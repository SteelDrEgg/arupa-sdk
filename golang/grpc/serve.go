package grpc

import (
	"context"
	"fmt"

	pluginv1 "github.com/SteelDrEgg/arupa-sdk/golang/gen/grpc"
	hcplugin "github.com/hashicorp/go-plugin"
	googlegrpc "google.golang.org/grpc"
)

const (
	pluginName       = "default_grpc"
	handshakeKey     = "ARUPA_PLUGIN"
	handshakeValue   = "arupa"
	handshakeVersion = 1
)

// Serve starts a gRPC plugin process using Arupa's fixed plugin runtime
// contract. It registers plugin as the sole Plugin service and does not
// return.
//
// The go-plugin handshake and dispense name are implementation details of the
// Arupa runtime. Use OnRegister to initialize application state from Params.
func Serve(plugin *Plugin) {
	ServeServer(plugin)
}

// ServeServer starts a gRPC plugin process for a custom implementation of the
// generated Plugin service. Most plugins should use Serve instead.
//
// This advanced entry point is useful only when a plugin needs to implement
// protocol RPCs itself rather than using Plugin's adapters.
func ServeServer(server pluginv1.PluginServer) {
	hcplugin.Serve(newServeConfig(server))
}

func newServeConfig(server pluginv1.PluginServer) *hcplugin.ServeConfig {
	return &hcplugin.ServeConfig{
		HandshakeConfig: hcplugin.HandshakeConfig{
			ProtocolVersion:  handshakeVersion,
			MagicCookieKey:   handshakeKey,
			MagicCookieValue: handshakeValue,
		},
		Plugins: map[string]hcplugin.Plugin{
			pluginName: &servedPlugin{server: server},
		},
		GRPCServer: hcplugin.DefaultGRPCServer,
	}
}

type servedPlugin struct {
	hcplugin.NetRPCUnsupportedPlugin

	server pluginv1.PluginServer
}

func (p *servedPlugin) GRPCServer(_ *hcplugin.GRPCBroker, server *googlegrpc.Server) error {
	if p.server == nil {
		return fmt.Errorf("arupa/grpc: plugin server is nil")
	}
	pluginv1.RegisterPluginServer(server, p.server)
	return nil
}

func (*servedPlugin) GRPCClient(context.Context, *hcplugin.GRPCBroker, *googlegrpc.ClientConn) (any, error) {
	return nil, fmt.Errorf("arupa/grpc: plugin process does not use GRPCClient")
}
