// Package main is the agent-bridge command entrypoint. It wires process
// signals to the application lifecycle and owns the process exit code.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/viethoangcr/agent-bridge/internal/app"
)

// runApp is the application seam invoked once by main. Tests replace it to
// exercise signal wiring and exit-code translation without starting the
// server.
var runApp = func(ctx context.Context) error {
	return app.Run(ctx, os.Getenv, app.IO{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
}

// run invokes runApp and returns the process exit code. Startup and runtime
// errors map to a nonzero exit; a cancellation-driven clean return maps to 0.
func run(ctx context.Context) int {
	if err := runApp(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(run(ctx))
}
