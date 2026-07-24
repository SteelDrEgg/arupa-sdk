//go:build ignore

// patch_wasm_host_responses fixes response ownership in code emitted by
// protoc-gen-go-plugin v0.9.0. Host responses are allocated through the
// guest's exported malloc function, so the guest must release each pointer
// after unmarshalling it.
package main

import (
	"bytes"
	"fmt"
	"os"
)

const expectedHostMethods = 13

var (
	unpatched = []byte("\tbuf = wasm.PtrToByte(ptr, size)\n\n\tresponse := new(")
	patched   = []byte("\tbuf = wasm.PtrToByte(ptr, size)\n\tdefer wasm.Free(ptr)\n\n\tresponse := new(")
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) != 1 {
		fail("usage: go run patch_wasm_host_responses.go <generated-plugin.go>")
	}
	path := args[0]
	source, err := os.ReadFile(path)
	if err != nil {
		fail("read %s: %v", path, err)
	}

	patchedCount := bytes.Count(source, patched)
	unpatchedCount := bytes.Count(source, unpatched)
	switch {
	case patchedCount == expectedHostMethods && unpatchedCount == 0:
		return
	case patchedCount != 0:
		fail("%s has %d patched and %d unpatched Host responses; want exactly %d patched",
			path, patchedCount, unpatchedCount, expectedHostMethods)
	case unpatchedCount != expectedHostMethods:
		fail("%s has %d unpatched Host responses; want %d",
			path, unpatchedCount, expectedHostMethods)
	}

	source = bytes.ReplaceAll(source, unpatched, patched)
	info, err := os.Stat(path)
	if err != nil {
		fail("stat %s: %v", path, err)
	}
	if err := os.WriteFile(path, source, info.Mode()); err != nil {
		fail("write %s: %v", path, err)
	}
}

func fail(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
