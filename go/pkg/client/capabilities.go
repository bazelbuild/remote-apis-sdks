package client

import (
	"context"
	"fmt"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"

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
	dgFn := digest.GetDigestFunction()
	if c.serverCaps.ExecutionCapabilities != nil {
		serverFn := c.serverCaps.ExecutionCapabilities.DigestFunction
		if serverFn == repb.DigestFunction_UNKNOWN {
			return fmt.Errorf("Server returned UNKNOWN digest function")
		}
		if serverFn != dgFn {
			return fmt.Errorf("Digest function mismatch: server requires %v, client uses %v", serverFn, dgFn)
		}
	}
	if c.serverCaps.CacheCapabilities != nil {
		cc := c.serverCaps.CacheCapabilities
		found := false
		for _, serverFn := range cc.DigestFunction {
			if serverFn == dgFn {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("Digest function mismatch: server requires one of %v, client uses %v", cc.DigestFunction, dgFn)
		}
		c.MaxBatchSize = MaxBatchSize(cc.MaxBatchTotalSizeBytes)
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
