// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is a dual-stack (IPv4 + IPv6) reimplementation of the stoppable TCP
// listener, derived from github.com/lni/goutils/netutil. The goutils version is
// IPv4-only: its parseAddress cannot handle "[::1]:port", and its resolver loop
// discards IPv6 addresses (v.To4()==nil). All address handling here uses net/netip;
// the net package is used only at the irreducible socket boundary (net.Listen and
// net.Conn). (Zuul fork patch — see PRP/security-audit-dragonboat.md.)

package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/lni/goutils/syncutil"
)

// errListenerStopped indicates that the listener has been stopped.
var errListenerStopped = errors.New("server stopped")

// stoppableListener is a dual-stack TCP listener that can be stopped by signalling
// stopc. It binds to every IP (IPv4 and IPv6) resolved from the supplied address.
type stoppableListener struct {
	listeners []net.Listener
	stopper   *syncutil.Stopper
	stopc     <-chan struct{}
	connc     chan net.Conn
	errc      chan error
	addr      string
}

// listenTargets resolves addr into the concrete ip:port strings to listen on. addr
// may be an IPv4 or IPv6 literal "ip:port" (including wildcards 0.0.0.0 / [::]) or a
// "host:port" whose host is resolved to every IPv4 AND IPv6 address it maps to.
func listenTargets(addr string) ([]string, error) {
	if ap, err := netip.ParseAddrPort(addr); err == nil {
		return []string{ap.String()}, nil
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil || len(host) == 0 {
		return nil, fmt.Errorf("invalid listen address %q", addr)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("invalid listen port in %q", addr)
	}
	addrs, err := net.DefaultResolver.LookupNetIP(context.Background(), "ip", host)
	if err != nil {
		return nil, err
	}
	seen := make(map[netip.Addr]struct{}, len(addrs))
	targets := make([]string, 0, len(addrs))
	for _, a := range addrs {
		a = a.Unmap()
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		targets = append(targets, netip.AddrPortFrom(a, uint16(port)).String())
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no addresses resolved for %q", addr)
	}
	return targets, nil
}

// newStoppableListener returns a dual-stack listener that can be stopped via stopc.
func newStoppableListener(addr string, tlsConfig *tls.Config,
	stopc <-chan struct{}) (*stoppableListener, error) {
	addr = strings.TrimSpace(addr)
	targets, err := listenTargets(addr)
	if err != nil {
		return nil, err
	}
	listeners := make([]net.Listener, 0, len(targets))
	for _, t := range targets {
		ln, err := net.Listen("tcp", t)
		if err != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
			return nil, err
		}
		listeners = append(listeners, ln)
	}
	s := &stoppableListener{
		listeners: listeners,
		stopper:   syncutil.NewStopper(),
		stopc:     stopc,
		addr:      addr,
		errc:      make(chan error, len(listeners)),
		connc:     make(chan net.Conn, len(listeners)),
	}
	for _, lis := range s.listeners {
		gl := lis
		s.stopper.RunWorker(func() {
			for {
				tc, err := gl.Accept()
				if err != nil {
					select {
					case s.errc <- err:
					case <-s.stopc:
						return
					}
					if isListenerStopperError(err) {
						return
					}
					continue
				}
				if tcpconn, ok := tc.(*net.TCPConn); ok {
					if err := setTCPConn(tcpconn); err != nil {
						continue
					}
				}
				if tlsConfig != nil {
					tc = tls.Server(tc, tlsConfig)
					if err := tc.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
						continue
					}
					if err := tc.(*tls.Conn).Handshake(); err != nil {
						continue
					}
				}
				select {
				case s.connc <- tc:
				case <-s.stopc:
					return
				}
			}
		})
	}
	return s, nil
}

func isListenerStopperError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}

// Accept accepts an incoming connection, or returns errListenerStopped once stopped.
func (ln *stoppableListener) Accept() (net.Conn, error) {
	select {
	case <-ln.stopc:
		var err error
		for _, v := range ln.listeners {
			if e := v.Close(); e != nil {
				err = e
			}
		}
		ln.stopper.Stop()
		if err == nil {
			err = errListenerStopped
		}
		return nil, err
	case err := <-ln.errc:
		return nil, err
	case c := <-ln.connc:
		return c, nil
	}
}

// Close closes the listener.
func (ln *stoppableListener) Close() error {
	for _, v := range ln.listeners {
		if err := v.Close(); err != nil {
			return err
		}
	}
	return nil
}
