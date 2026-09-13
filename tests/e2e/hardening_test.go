//go:build e2e

package e2e

import "testing"

// TestDockerHardeningContract is the cross-cutting release gate for the
// authoritative security, error, logging, child-environment, and limit
// boundaries. Image-level structural checks that need no live server run
// first; the live checks each own the container they create. Expensive real
// boundaries (10MiB ACP, 512MiB filesystem, 8KiB stderr) stay in their existing
// owning tests and are deliberately not repeated here. Per-concern bodies live
// in the sibling hardening files.
func TestDockerHardeningContract(t *testing.T) {
	image := buildImage(t)

	t.Run("image", func(t *testing.T) { assertImageContract(t, image) })
	t.Run("http", func(t *testing.T) { assertHTTPContract(t, image) })
	t.Run("insecure-remote", func(t *testing.T) { assertInsecureRemoteContract(t, image) })
	t.Run("child-env", func(t *testing.T) { assertChildEnvContract(t, image) })
	t.Run("limits", func(t *testing.T) { assertInjectedLimits(t, image) })
}
