package balancer

import (
	"context"
	"os"
	"sync"

	pb "github.com/bazelbuild/remote-apis-sdks/go/pkg/balancer/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	// Default max number of connections is 0, meaning "no limit"
	defaultMaxConn = 0

	// Default max stream watermark is 100, which is the current stream limit for GFE.
	// Any value >100 will be rounded down to 100.
	defaultMaxStream = 100
)

type key int

var gcpKey key

type poolConfig struct {
	maxConn   uint32
	maxStream uint32
}

type gcpContext struct {
	affinityCfg *pb.AffinityConfig
	poolCfg     *poolConfig
	// request message used for pre-process of an affinity call
	reqMsg interface{}
	// response message used for post-process of an affinity call
	replyMsg interface{}
}

// GCPInterceptor provides functions for intercepting client requests
// in order to support GCP specific features
type GCPInterceptor struct {
	poolCfg *poolConfig

	// Maps method path to AffinityConfig
	methodToAffinity map[string]*pb.AffinityConfig
}

// NewGCPInterceptor creates a new GCPInterceptor with a given ApiConfig
func NewGCPInterceptor(config *pb.ApiConfig) *GCPInterceptor {
	mp := make(map[string]*pb.AffinityConfig)
	methodCfgs := config.GetMethod()
	for _, methodCfg := range methodCfgs {
		methodNames := methodCfg.GetName()
		affinityCfg := methodCfg.GetAffinity()
		if methodNames != nil && affinityCfg != nil {
			for _, method := range methodNames {
				mp[method] = affinityCfg
			}
		}
	}

	poolCfg := &poolConfig{
		maxConn:   defaultMaxConn,
		maxStream: defaultMaxStream,
	}

	userPoolCfg := config.GetChannelPool()

	// Set user defined MaxSize.
	poolCfg.maxConn = userPoolCfg.GetMaxSize()

	// Set user defined MaxConcurrentStreamsLowWatermark if ranged in [1, defaultMaxStream],
	// otherwise use the defaultMaxStream.
	watermarkValue := userPoolCfg.GetMaxConcurrentStreamsLowWatermark()
	if watermarkValue >= 1 && watermarkValue <= defaultMaxStream {
		poolCfg.maxStream = watermarkValue
	}
	return &GCPInterceptor{
		poolCfg:          poolCfg,
		methodToAffinity: mp,
	}
}

// GCPUnaryClientInterceptor intercepts the execution of a unary RPC
// and injects necessary information to be used by the picker.
func (gcpInt *GCPInterceptor) GCPUnaryClientInterceptor(
	ctx context.Context,
	method string,
	req interface{},
	reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	affinityCfg, _ := gcpInt.methodToAffinity[method]
	gcpCtx := &gcpContext{
		affinityCfg: affinityCfg,
		reqMsg:      req,
		replyMsg:    reply,
		poolCfg:     gcpInt.poolCfg,
	}
	ctx = context.WithValue(ctx, gcpKey, gcpCtx)

	return invoker(ctx, method, req, reply, cc, opts...)
}

// GCPStreamClientInterceptor intercepts the execution of a client streaming RPC
// and injects necessary information to be used by the picker.
func (gcpInt *GCPInterceptor) GCPStreamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	// This constructor does not create a real ClientStream,
	// it only stores all parameters and let SendMsg() to create ClientStream.
	affinityCfg, _ := gcpInt.methodToAffinity[method]
	gcpCtx := &gcpContext{
		affinityCfg: affinityCfg,
		poolCfg:     gcpInt.poolCfg,
	}
	ctx = context.WithValue(ctx, gcpKey, gcpCtx)
	cs := &gcpClientStream{
		gcpInt:   gcpInt,
		ctx:      ctx,
		desc:     desc,
		cc:       cc,
		method:   method,
		streamer: streamer,
		opts:     opts,
	}
	cs.cond = sync.NewCond(cs)
	return cs, nil
}

type gcpClientStream struct {
	sync.Mutex
	grpc.ClientStream

	cond          *sync.Cond
	initStreamErr error
	gcpInt        *GCPInterceptor
	ctx           context.Context
	desc          *grpc.StreamDesc
	cc            *grpc.ClientConn
	method        string
	streamer      grpc.Streamer
	opts          []grpc.CallOption
}

func (cs *gcpClientStream) SendMsg(m interface{}) error {
	cs.Lock()
	// Initialize underlying ClientStream when getting the first request.
	if cs.ClientStream == nil {
		affinityCfg, ok := cs.gcpInt.methodToAffinity[cs.method]
		ctx := cs.ctx
		if ok {
			gcpCtx := &gcpContext{
				affinityCfg: affinityCfg,
				reqMsg:      m,
				poolCfg:     cs.gcpInt.poolCfg,
			}
			ctx = context.WithValue(cs.ctx, gcpKey, gcpCtx)
		}
		realCS, err := cs.streamer(ctx, cs.desc, cs.cc, cs.method, cs.opts...)
		if err != nil {
			cs.initStreamErr = err
			cs.Unlock()
			cs.cond.Broadcast()
			return err
		}
		cs.ClientStream = realCS
	}
	cs.Unlock()
	cs.cond.Broadcast()
	return cs.ClientStream.SendMsg(m)
}

func (cs *gcpClientStream) RecvMsg(m interface{}) error {
	// If RecvMsg is called before SendMsg, it should wait until cs.ClientStream
	// is initialized or the initialization failed.
	cs.Lock()
	for cs.initStreamErr == nil && cs.ClientStream == nil {
		cs.cond.Wait()
	}
	if cs.initStreamErr != nil {
		cs.Unlock()
		return cs.initStreamErr
	}
	cs.Unlock()
	return cs.ClientStream.RecvMsg(m)
}

// ParseAPIConfig parses a json config file into ApiConfig proto message.
func ParseAPIConfig(path string) (*pb.ApiConfig, error) {
	jsonFile, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	result := &pb.ApiConfig{}
	protojson.Unmarshal(jsonFile, result)
	return result, nil
}
