package client

import (
	"context"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/pkg/errors"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// CheckCapabilities verifies that this client can work with the remote server
// in terms of API version and digest function. It sets some client parameters
// according to remote server preferences, like MaxBatchSize.
func (c *Client) CheckCapabilities(ctx context.Context) (err error) {
	// Only query the server once. There is no need for a lock, because we will
	// usually make the call on startup.
	if c.serverCaps == nil {
		if c.serverCaps, err = c.GetCapabilities(ctx); err != nil {
			return err
		}
	}

	if err := digest.CheckCapabilities(c.serverCaps); err != nil {
		return errors.Wrapf(err, "digest function mismatch")
	}

	if c.serverCaps.CacheCapabilities != nil {
		c.MaxBatchSize = MaxBatchSize(c.serverCaps.CacheCapabilities.MaxBatchTotalSizeBytes)
	}

	if useCompression := c.CompressedBytestreamThreshold >= 0; useCompression {
		if c.serverCaps.CacheCapabilities.SupportedCompressors == nil {
			return errors.New("the server does not support compression")
		}

		foundZstd := false
		for _, sComp := range c.serverCaps.CacheCapabilities.SupportedCompressors {
			if sComp == repb.Compressor_ZSTD {
				foundZstd = true
				break
			}
		}
		if !foundZstd {
			return errors.New("zstd is not supported by server, while the SDK only supports ZSTD compression")
		}
		for _, compressor := range c.serverCaps.CacheCapabilities.SupportedBatchUpdateCompressors {
			if compressor == repb.Compressor_ZSTD {
				c.useBatchCompression = UseBatchCompression(true)
			}
		}
	}
	return nil
}

// GetCapabilities returns the capabilities for the targeted servers.
// If the CAS URL was set differently to the execution server then the CacheCapabilities will
// be determined from that; ExecutionCapabilities will always come from the main URL.
func (c *Client) GetCapabilities(ctx context.Context) (res *repb.ServerCapabilities, err error) {
	return c.GetCapabilitiesForInstance(ctx, c.InstanceName)
}

// GetCapabilitiesForInstance returns the capabilities for the targeted servers.
// If the CAS URL was set differently to the execution server then the CacheCapabilities will
// be determined from that; ExecutionCapabilities will always come from the main URL.
func (c *Client) GetCapabilitiesForInstance(ctx context.Context, instance string) (res *repb.ServerCapabilities, err error) {
	req := &repb.GetCapabilitiesRequest{InstanceName: instance}
	caps, err := c.GetBackendCapabilities(ctx, c.Connection, req)
	if err != nil {
		return nil, err
	}
	if c.CASConnection != c.Connection {
		casCaps, err := c.GetBackendCapabilities(ctx, c.CASConnection, req)
		if err != nil {
			return nil, err
		}
		caps.CacheCapabilities = casCaps.CacheCapabilities
	}
	return caps, nil
}

// SupportsActionPlatformProperties returns whether the server's RE API version
// supports the `Action.platform_properties` field.
func (c *Client) SupportsActionPlatformProperties() bool {
	return supportsActionPlatformProperties(c.serverCaps)
}

// SupportsCommandOutputPaths returns whether the server's RE API version
// supports the `Command.action_paths` field.
func (c *Client) SupportsCommandOutputPaths() bool {
	return supportsCommandOutputPaths(c.serverCaps)
}

// HighAPIVersionNewerThanOrEqualTo returns whether the latest version reported
// as supported in ServerCapabilities matches or is more recent than a
// reference major/minor version.
func highAPIVersionNewerThanOrEqualTo(serverCapabilities *repb.ServerCapabilities, major int32, minor int32) bool {
	if serverCapabilities == nil {
		return false
	}

	latestSupportedMajor := serverCapabilities.HighApiVersion.GetMajor()
	latestSupportedMinor := serverCapabilities.HighApiVersion.GetMinor()

	return (latestSupportedMajor > major || (latestSupportedMajor == major && latestSupportedMinor >= minor))
}

func supportsActionPlatformProperties(serverCapabilities *repb.ServerCapabilities) bool {
	// According to the RE API spec:
	// "New in version 2.2: clients SHOULD set these platform properties as well
	// as those in the [Command][build.bazel.remote.execution.v2.Command].
	// Servers SHOULD prefer those set here."
	return highAPIVersionNewerThanOrEqualTo(serverCapabilities, 2, 2)
}

func supportsCommandOutputPaths(serverCapabilities *repb.ServerCapabilities) bool {
	// According to the RE API spec:
	// "[In v2.1 `output_paths`] supersedes the DEPRECATED `output_files` and
	// `output_directories` fields. If `output_paths` is used, `output_files` and
	// `output_directories` will be ignored!"
	return highAPIVersionNewerThanOrEqualTo(serverCapabilities, 2, 1)
}
