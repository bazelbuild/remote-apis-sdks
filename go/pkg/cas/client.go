// Package cas implements an efficient client for Content Addressable Storage.
package cas

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// Client is a client for Content Addressable Storage.
// Create one using NewClient.
//
// Goroutine-safe.
//
// All fields are considered immutable, and should not be changed.
type Client struct {
	// The full name of the RBE instance.
	InstanceName string

	// The configuration that the client was created with.
	ClientConfig

	conn       *grpc.ClientConn
	byteStream bspb.ByteStreamClient
	cas        repb.ContentAddressableStorageClient
}

// ClientConfig is a config for Client.
// See DefaultClientConfig() for the default values.
type ClientConfig struct {
	DialParams client.DialParams

	// TODO(nodir): add conifg values here.
	// TODO(nodir): add per-RPC timeouts.
	// TODO(nodir): add retries.
}

// DefaultClientConfig returns the default config.
//
// To override a specific value:
//   cfg := DefaultClientConfig()
//   ... mutate cfg ...
//   client, err := NewClientWithConfig(ctx, cfg)
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		DialParams: client.DialParams{
			Service: "remotebuildexecution.googleapis.com:443",
		},
	}
}

// Validate returns a non-nil error if the config is invalid.
func (c *ClientConfig) Validate() error {
	if c.DialParams.Service == "" {
		return errors.Errorf("DialParams.Service is unspecified")
	}

	return nil
}

// NewClient creates a new client with the default configuration.
func NewClient(ctx context.Context, instanceName string) (*Client, error) {
	return NewClientWithConfig(ctx, instanceName, DefaultClientConfig())
}

// NewClientWithConfig creates a new client and accepts a configuration.
func NewClientWithConfig(ctx context.Context, instanceName string, config ClientConfig) (*Client, error) {
	if instanceName == "" {
		return nil, fmt.Errorf("instance name is unspecified")
	}
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	conn, err := client.Dial(ctx, config.DialParams.Service, config.DialParams)
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial")
	}

	client := &Client{
		InstanceName: instanceName,
		ClientConfig: config,
		conn:         conn,
		byteStream:   bspb.NewByteStreamClient(conn),
		cas:          repb.NewContentAddressableStorageClient(conn),
	}

	// TODO(nodir): Check capabilities.
	return client, nil
}
