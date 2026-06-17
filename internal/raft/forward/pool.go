package forward

import (
	"github.com/gostdlib/base/concurrency/sync"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/raft/forward/forwardpb"
)

// errPoolClosed indicates the dispatcher was closed; no new peer connections.
var errPoolClosed = errors.New("forward: dispatcher closed")

// pool is a lazily-dialed cache of Forwarder client connections, keyed by address.
// grpc.NewClient is lazy, so a connection is established on first use, not here.
type pool struct {
	dialOpts []grpc.DialOption
	mu       sync.Mutex
	conns    map[string]*grpc.ClientConn
	closed   bool
}

// newPool returns a pool that dials peers with dialOpts (e.g. mutual-TLS transport
// credentials). When none are given, an insecure connection is used.
func newPool(dialOpts []grpc.DialOption) *pool {
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &pool{dialOpts: dialOpts, conns: map[string]*grpc.ClientConn{}}
}

// client returns a Forwarder client for addr, dialing (lazily) and caching the
// connection on first request.
func (p *pool) client(addr string) (forwardpb.ForwarderClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errPoolClosed // a late caller must not recreate (and leak) a conn
	}
	cc := p.conns[addr]
	if cc == nil {
		var err error
		cc, err = grpc.NewClient(addr, p.dialOpts...)
		if err != nil {
			return nil, err
		}
		p.conns[addr] = cc
	}
	return forwardpb.NewForwarderClient(cc), nil
}

// evict closes and drops the cached connection to addr, if any, so the next request to
// it dials afresh. It reclaims a connection to a peer that has gone away (e.g. a
// decommissioned node whose address never recovers), which would otherwise linger until
// close. A no-op if addr is not cached or the pool is closed.
func (p *pool) evict(addr string) {
	p.mu.Lock()
	cc := p.conns[addr]
	delete(p.conns, addr)
	p.mu.Unlock()
	if cc != nil {
		_ = cc.Close()
	}
}

// close shuts every cached connection and refuses new ones.
func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, cc := range p.conns {
		_ = cc.Close()
	}
	p.conns = nil
}
