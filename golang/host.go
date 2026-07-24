package arupa

import "context"

// HostEmitter sends a Socket.IO emit through the host control plane.
type HostEmitter interface {
	Emit(context.Context, EmitInstruction) error
}

// HostClient is the complete backend-neutral host capability set available to
// a registered service.
type HostClient interface {
	KVClient
	ParamsClient
	Logger
	HostEmitter
	ServiceMessageSender
	ResourceRegistrar
}
