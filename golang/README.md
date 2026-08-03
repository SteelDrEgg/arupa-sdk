# Arupa Go SDK — Service contract v2

This module implements Arupa Service contract v2 for native gRPC services and
WASI modules. Contract v2 separates stable service identity from runtime
resources:

- `arupa.ServiceInfo` supplies the name and version returned by
  `Service.Register`.
- Transports and routes are created dynamically through the Host API.
- The Host API is available before `OnRegister` runs.
- The host forwards HTTP requests, Socket.IO events, and service messages
  through the same four Service callbacks on both backends.

The package root contains backend-neutral types and handlers. Use
`github.com/SteelDrEgg/arupa-sdk/golang/grpc` for a native subprocess or
`github.com/SteelDrEgg/arupa-sdk/golang/wasm` for a WASI module.

## Minimal gRPC service

Any standard `http.Handler` can handle host-forwarded HTTP requests. This
includes `http.ServeMux` and framework engines such as Gin, Chi, and Echo.

```go
package main

import (
	"fmt"
	"net/http"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	arupagrpc "github.com/SteelDrEgg/arupa-sdk/golang/grpc"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "hello from users")
	})

	service := &arupagrpc.Service{
		Info: arupa.ServiceInfo{
			Name:    "users",
			Version: "1.0.0",
		},
		Handler: mux,
	}

	arupagrpc.Serve(service)
}
```

`Serve` configures the contract-v2 handshake and starts the service
subprocess. Most services should use `arupagrpc.Service`; the lower-level
`arupagrpc.ServeServer` entry point is available for a custom generated
`ServiceServer`.

## Dynamic transports and routes

Identity registration does not publish ingress by itself. Register a transport
and then bind one or more routes to it. The Host connection is already active
when `OnRegister` runs, so startup resource registration can happen there:

```go
service.OnRegister = func(ctx context.Context) error {
	result, err := service.RegisterTransport(ctx, arupa.Transport{
		ID:   "http",
		Type: arupa.TransportHTTP,
	})
	if err := requireRegistration("register HTTP transport", result, err); err != nil {
		return err
	}

	result, err = service.RegisterRoutes(ctx, []arupa.Route{
		{
			ID:          "users-api",
			TransportID: "http",
			HTTP: &arupa.HTTPRoute{
				Method:  "GET",
				Pattern: "/api/users/",
				Access: arupa.AccessPolicy{
					RequireAuth: true,
					Groups:      []string{"users.read"},
				},
			},
		},
	})
	return requireRegistration("register HTTP routes", result, err)
}

func requireRegistration(
	operation string,
	result arupa.RegistrationResult,
	err error,
) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if !result.Successful() {
		return fmt.Errorf(
			"%s: message=%q degraded=%t failures=%v",
			operation,
			result.Message,
			result.Degraded,
			result.Failures,
		)
	}
	return nil
}
```

The example needs `context` and `fmt` imports in addition to the minimal
service imports.

Available transport types are:

- `arupa.TransportStatic`: serves a file or directory below the extracted
  service `Content` root. `StaticSource` must be a relative path.
- `arupa.TransportHTTP`: forwards matching requests to `HandleHTTP`.
- `arupa.TransportSocketIO`: forwards declared Socket.IO events to
  `HandleSocketEvent`.
- `arupa.TransportProxy`: streams HTTP, WebSocket, or Socket.IO traffic to an
  inherited, Unix, or TCP HTTP listener.

### HTTP route rewriting

Static, HTTP RPC, and proxy routes all preserve the matched route prefix by
default. Set the route-level rewrite rule when the downstream handler is
mounted at `/` instead of at the external route prefix:

```go
HTTP: &arupa.HTTPRoute{
	Pattern: "/app/",
	Rewrite: arupa.RewriteRule{
		Prefix:   true,
		Location: true,
	},
},
```

With this rule, `/app/users?q=1` is dispatched as `/users?q=1`. The kernel
sets the trusted `X-Forwarded-Prefix: /app` header for HTTP RPC and proxy
requests. `Location` makes the kernel rewrite a root-relative downstream
header such as `Location: /login` to `Location: /app/login`; relative,
scheme-relative, and absolute locations are unchanged.

Both options are ordinary booleans and default to false. `Location: true`
requires `Prefix: true`. Static transports have no separate strip-prefix
setting; the route-level rule is the common mechanism for all three HTTP
transport types.

`RegistrationResult` preserves partial batch outcomes. A non-nil Go error
means the Host call failed or a declaration could not be encoded; item-level
rejections appear in `Registered`, `Failures`, `Degraded`, and `Message`.
Successful items in a partial batch remain active.

Resources can also be changed after startup. Remove routes before removing the
transport they reference:

```go
result, err := service.UnregisterRoutes(ctx, []string{"users-api"})
if err := requireRegistration("unregister routes", result, err); err != nil {
	return err
}

result, err = service.UnregisterTransport(ctx, "http")
if err := requireRegistration("unregister transport", result, err); err != nil {
	return err
}
```

## Host capabilities

Both `*grpc.Service` and `*wasm.Service` implement `arupa.HostClient` and expose
the same Host-backed operations directly. Invoke them only after registration
has begun; `OnRegister` is the first hook where every Host capability is
available.

### Params

`InitialParams` is an immutable copy of the values in the latest register
request. `Params` reads the current effective value from the Host, and
`PatchParams` persists additions, replacements, and deletions:

```go
initial := service.InitialParams()

current, err := service.Params(ctx)
if err != nil {
	return err
}

err = service.PatchParams(ctx, arupa.ParamsPatch{
	Set: map[string]string{
		"theme": "dark",
	},
	Delete: []string{"obsolete"},
})
```

### KV

`KV` returns a store scoped to `ServiceInfo.Name`:

```go
store := service.KV()

if err := store.Set(ctx, "user:42", []byte("cached value")); err != nil {
	return err
}

value, found, err := store.Get(ctx, "user:42")
keys, err := store.List(ctx)
err = store.Delete(ctx, "user:42")
```

The lower-level `KVGet`, `KVSet`, and `KVDelete` methods address an explicit
namespace. `KVList(ctx, namespace)` lists keys in one namespace, while
`KVList(ctx, "")` lists all namespace names exposed by the Host.

### Host logging and Socket.IO emits

Use `Log` with an `arupa.LogLevel`, or one of the convenience methods
`LogDebug`, `LogInfo`, `LogWarn`, and `LogError`:

```go
if err := service.LogInfo(ctx, "service is ready"); err != nil {
	return err
}

if err := service.EmitJSON(
	ctx,
	"/users",
	"",
	"ready",
	map[string]any{"version": service.Info.Version},
); err != nil {
	return err
}
```

An empty emit target broadcasts within the namespace. A non-empty target can
identify the destination selected by the Host.

For incoming Socket.IO events, install one listener registry on the service.
Emits made through the handler-scoped `arupa.Emitter` are returned as part of
that event reply:

```go
events := arupa.NewSocketListener()
if err := events.On(
	"message",
	func(ctx context.Context, event arupa.SocketEvent, emit arupa.Emitter) error {
		return arupa.EmitJSON(
			emit,
			event.Namespace,
			event.SocketID,
			"message.received",
			map[string]string{"route": event.RouteID},
		)
	},
); err != nil {
	panic(err)
}

service.Events = events
```

Declare the corresponding Socket.IO ingress dynamically:

```go
result, err := service.RegisterTransport(ctx, arupa.Transport{
	ID:   "socket",
	Type: arupa.TransportSocketIO,
})
if err := requireRegistration("register Socket.IO transport", result, err); err != nil {
	return err
}

result, err = service.RegisterRoutes(ctx, []arupa.Route{
	{
		ID:          "users-socket",
		TransportID: "socket",
		SocketIO: &arupa.SocketIORoute{
			Namespace: "/users",
			Events:    []string{"message"},
			Access: arupa.AccessPolicy{
				RequireAuth: true,
			},
		},
	},
})
```

### Service messages

Service messages are request/reply messages between registered services. An
incoming topic has one handler, with an optional fallback:

```go
messages := arupa.NewServiceMessageListener()
if err := messages.On(
	"cache.invalidate",
	func(ctx context.Context, message arupa.IncomingServiceMessage) (string, error) {
		// message.Source is authenticated and supplied by the Host.
		return "ok", nil
	},
); err != nil {
	panic(err)
}

service.Messages = messages
```

Send raw bytes with `SendServiceMessage`, or let `SendServiceJSON` encode a
payload:

```go
reply, err := service.SendServiceJSON(
	ctx,
	"cache",
	"cache.invalidate",
	map[string]string{"key": "user:42"},
)
```

## HTTP route context and headers

The selected Host route is separate from routing inside the HTTP framework.
Read its stable ID and registered pattern with `HTTPRouteFromContext`. An
authenticated user, when present, is available through `UserFromContext`:

```go
mux.HandleFunc("/api/users/", func(w http.ResponseWriter, r *http.Request) {
	route, matched := arupa.HTTPRouteFromContext(r.Context())
	user, authenticated := arupa.UserFromContext(r.Context())

	// Header.Values returns every value forwarded by the Host.
	traceValues := r.Header.Values("X-Trace")

	// Multiple response values are preserved as separate protocol values.
	w.Header().Add("Set-Cookie", "theme=dark; Path=/")
	w.Header().Add("Set-Cookie", "locale=en; Path=/")

	fmt.Fprintf(
		w,
		"route=%s pattern=%s matched=%t user=%v authenticated=%t traces=%v",
		route.ID,
		route.Pattern,
		matched,
		user,
		authenticated,
		traceValues,
	)
})
```

The gRPC and WASM adapters preserve all values for every request and response
header. Use `Header.Values` or direct slice access when repeated values such as
`Cookie`, `Set-Cookie`, or forwarding metadata matter. When prefix rewriting
is enabled, read the trusted value with
`r.Header.Get(arupa.ForwardedPrefixHeader)`. The kernel removes any
caller-supplied `X-Forwarded-Prefix`; when rewriting is disabled, the header is
absent.

## Inherited listener for a native proxy service

For gRPC services, the Host can create a listener before starting the child
process and pass it in the register request. The SDK safely adopts the file
descriptor and exposes it as a `net.Listener` by ID:

```go
service.OnRegister = func(ctx context.Context) error {
	listener, ok := service.InheritedListener("proxy")
	if !ok {
		return fmt.Errorf("Host did not provide the proxy listener")
	}

	result, err := service.RegisterTransport(ctx, arupa.Transport{
		ID:   "proxy",
		Type: arupa.TransportProxy,
		Proxy: &arupa.ProxyTarget{
			Network: arupa.ProxyInherited,
			Address: "proxy",
			Scheme:  "http",
		},
	})
	if err := requireRegistration("register proxy transport", result, err); err != nil {
		return err
	}

	result, err = service.RegisterRoutes(ctx, []arupa.Route{
		{
			ID:          "proxy-api",
			TransportID: "proxy",
			HTTP: &arupa.HTTPRoute{
				Pattern: "/proxy/",
			},
		},
	})
	if err := requireRegistration("register proxy route", result, err); err != nil {
		return err
	}

	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "served through the inherited listener")
	})
	go func() {
		err := http.Serve(listener, upstream)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = service.LogError(context.Background(), err.Error())
		}
	}()
	return nil
}
```

This example additionally imports `errors`. Do not reopen or close the raw
descriptor from `InitialRegisterContext`; the returned
`*grpc.InheritedListener` owns the adopted descriptor. `Service.Close` closes
all inherited listeners and the reverse Host connection and is safe to call
more than once.

Inherited OS listeners are a native gRPC capability. A WASI module should use
the callback-based `http` and `socket.io` transports.

## WASM service

The WASM adapter has the same `Service` fields and Host-facing methods. Register
the service during module initialization and keep `main` empty:

```go
package main

import (
	"context"
	"net/http"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	arupawasm "github.com/SteelDrEgg/arupa-sdk/golang/wasm"
)

var service = &arupawasm.Service{
	Info: arupa.ServiceInfo{
		Name:    "users",
		Version: "1.0.0",
	},
	Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}),
}

func init() {
	service.OnRegister = func(ctx context.Context) error {
		result, err := service.RegisterTransport(ctx, arupa.Transport{
			ID:   "http",
			Type: arupa.TransportHTTP,
		})
		if err := requireRegistration("register HTTP transport", result, err); err != nil {
			return err
		}

		result, err = service.RegisterRoutes(ctx, []arupa.Route{
			{
				ID:          "users-api",
				TransportID: "http",
				HTTP: &arupa.HTTPRoute{
					Pattern: "/api/users/",
				},
			},
		})
		return requireRegistration("register HTTP routes", result, err)
	}

	arupawasm.RegisterService(service)
}

func main() {}
```

The example reuses `requireRegistration` from the dynamic registration
section. Build it as a WASI module:

```sh
CGO_ENABLED=0 GOOS=wasip1 GOARCH=wasm \
  go build -buildmode=c-shared -o users.wasm .
```

The generated WASM ABI imports all Host capabilities, so Params, KV, logging,
emits, service messages, and dynamic resource registration are available
inside `OnRegister` just as they are in the gRPC adapter.

## `.plg` package

A service package is a ZIP archive with a `.plg` suffix. `info.yaml` is at the
archive root, while the executable, WASM module, and service-owned resources
are below `Content/`. `$PLUGIN_ROOT` resolves to the extracted `Content`
directory.

For the native service:

```yaml
Name: users
Version: 1.0.0
Type: grpc
ContractVersion: 2
Command: $PLUGIN_ROOT/users-service
```

For the WASI module:

```yaml
Name: users
Version: 1.0.0
Type: wasm
ContractVersion: 2
Command: $PLUGIN_ROOT/users.wasm
```

The archive layouts are:

```text
users-grpc.plg
├── info.yaml
└── Content/
    └── users-service

users-wasm.plg
├── info.yaml
└── Content/
    └── users.wasm
```

`ContractVersion` must be `2`. The `Name` and `Version` in `info.yaml` must
exactly match the values returned from the service's `ServiceInfo`.
