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

plugin := &arupagrpc.Plugin{
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

arupagrpc.Serve(plugin)
```

## Registration hook and Params

`Plugin.OnRegister` runs once for each loaded plugin instance, after the host
callback and `InitialParams` snapshot are available. Return an error to reject
registration. Use `Params` when the plugin needs the host's current effective
configuration; use `InitialParams` when it specifically needs the Params sent
with this registration request.

```go
plugin.OnRegister = func(ctx context.Context) error {
	params, err := plugin.Params(ctx)
	if err != nil {
		return err
	}
	return service.Configure(params)
}
```

For a custom `PluginServer`, use the adapter directly and start it with the
advanced `ServeServer` entry point:

```go
func (p *Plugin) HandleHTTP(ctx context.Context, req *pluginv1.HTTPRequest) (*pluginv1.HTTPResponse, error) {
    return arupagrpc.ServeHTTP(ctx, req, p.app)
}

func main() {
	arupagrpc.ServeServer(&Plugin{})
}
```

## WASM

The generated WASM ABI is in `gen/wasm/proto`; its thin protocol binding is in
the `wasm` package. `wasm.Plugin` uses the same `arupa.Registration` and
shared HTTP implementation as `arupagrpc.Plugin`. Compile the plugin for
`wasip1` and export it with:

```go
wasm.Register(plugin)
```

## Socket events

Socket namespaces remain host declarations in `arupa.Registration`. To listen
for forwarded events, create one event bus and register functions on it:

```go
events := arupa.NewSocketListener()
_ = events.On("message", func(ctx context.Context, event arupa.SocketEvent, emit arupa.Emitter) error {
    return arupa.EmitJSON(emit, event.Namespace, event.SocketID, "reply", "received")
})

plugin.Events = events
```

Every `EmitJSON` performed while handling an event is returned to the host in
that event's `SocketEventReply`, for both gRPC and WASM plugins.

For a gRPC plugin, emits can also be sent after event handling, including from
background work. `Plugin.Register` establishes the callback internally:

```go
_ = plugin.EmitJSON(ctx, "/chat", "", "notice", "server is ready")
```

## Plugin messages

Plugin messages use one listener per request/reply topic:

```go
messages := arupa.NewMessageListener()
_ = messages.On("cache.invalidate", func(ctx context.Context, message arupa.IncomingMessage) (string, error) {
    return "ok", nil
})

plugin.Messages = messages

reply, err := plugin.SendJSON(ctx, "cache", "cache.invalidate", map[string]string{
	"key": "user:42",
})
```

## KV storage

`Plugin.KV()` scopes storage to the plugin's registered name, so no namespace
needs to be passed by application code. Empty namespaces are rejected by the
SDK, including when using the lower-level `KV*` methods.

```go
store := plugin.KV()
if err := store.Set(ctx, "user:42", []byte("cached value")); err != nil {
	return err
}

value, found, err := store.Get(ctx, "user:42")
```
