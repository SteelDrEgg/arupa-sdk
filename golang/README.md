# Arupa Go SDK

`arupa.Registration` declares the plugin information that the host needs
during `Plugin.Register`. Its HTTP routes are host ingress and authorization
rules; they are not framework routes.

`arupa.ServeHTTP` is the single framework-neutral HTTP implementation. The
`grpc` and `wasm` packages only convert generated protocol types at their
respective boundaries; neither implements its own HTTP adapter.

Gin, Chi, and Echo engines implement `http.Handler`, so they can be passed to
the adapter unchanged.

```go
app := gin.Default()
app.GET("/api/users/:id", getUser)

plugin := &arupagrpc.HTTPPlugin{
    Registration: arupa.Registration{
        Name:    "users",
        Version: "1.0.0",
        HTTPRoutes: []arupa.HTTPRoute{
            {
                Pattern: "/api/",
                Access:  arupa.AccessPolicy{RequireAuth: true},
            },
        },
        SocketNamespaces: []arupa.SocketNamespace{
            {Name: "/users"},
        },
    },
    Handler: app,
}

pluginv1.RegisterPluginServer(grpcServer, plugin)
```

For a custom `PluginServer`, use the adapter directly:

```go
func (p *Plugin) HandleHTTP(ctx context.Context, req *pluginv1.HTTPRequest) (*pluginv1.HTTPResponse, error) {
    return arupagrpc.ServeHTTP(ctx, req, p.app)
}
```

## WASM

The generated WASM ABI is in `gen/wasm/proto`; its thin protocol binding is in
the `wasm` package. `wasm.HTTPPlugin` uses the same `arupa.Registration` and
shared HTTP implementation as `arupagrpc.HTTPPlugin`. Compile the plugin for
`wasip1` and export it with:

```go
wasm.RegisterPlugin(plugin)
```
