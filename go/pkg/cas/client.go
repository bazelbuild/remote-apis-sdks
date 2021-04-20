// Package cas implements an efficient client for Content Addressable Storage.
package cas

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// Client is a client for Content Addressable Storage.
// Create one using NewClient.
//
// Goroutine-safe.
//
// All fields are considered immutable, and should not be changed.
type Client struct {
	conn *grpc.ClientConn
	// The full name of the RBE instance.
	InstanceName string

	// The configuration that the client was created with.
	ClientConfig

	byteStream bspb.ByteStreamClient
	cas        repb.ContentAddressableStorageClient
}

// ClientConfig is a config for Client.
// See DefaultClientConfig() for the default values.
type ClientConfig struct {
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
	return ClientConfig{}
}

// Validate returns a non-nil error if the config is invalid.
func (c *ClientConfig) Validate() error {
	return nil
}

// NewClient creates a new client with the default configuration.
// Use client.Dial to create a connection.
func NewClient(ctx context.Context, conn *grpc.ClientConn, instanceName string) (*Client, error) {
	return NewClientWithConfig(ctx, conn, instanceName, DefaultClientConfig())
}

// NewClientWithConfig creates a new client and accepts a configuration.
func NewClientWithConfig(ctx context.Context, conn *grpc.ClientConn, instanceName string, config ClientConfig) (*Client, error) {
	switch err := config.Validate(); {
	case err != nil:
		return nil, errors.Wrap(err, "invalid config")
	case conn == nil:
		return nil, fmt.Errorf("conn is unspecified")
	case instanceName == "":
		return nil, fmt.Errorf("instance name is unspecified")
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
