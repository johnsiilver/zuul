// Package cmd builds the FSM command messages (fsmpb.Command) that the server,
// session manager, and consensus layer propose into Raft. Centralizing the
// constructors keeps the oneof wrapping in one place.
package cmd

import "github.com/johnsiilver/zuul/internal/fsm/fsmpb"

// Acquire builds an AcquireLock command (try == true means non-blocking).
func Acquire(name, clientID string, try bool) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_AcquireLock{AcquireLock: &fsmpb.AcquireLock{Name: name, ClientId: clientID, TryLock: try}}}
}

// Release builds a ReleaseLock command.
func Release(name, clientID string, token uint64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_ReleaseLock{ReleaseLock: &fsmpb.ReleaseLock{Name: name, ClientId: clientID, FencingToken: token}}}
}

// Cancel builds a CancelWait command.
func Cancel(name, clientID string) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_CancelWait{CancelWait: &fsmpb.CancelWait{Name: name, ClientId: clientID}}}
}

// LeaseGrant builds a LeaseGrant command stamped with the leader's clock.
func LeaseGrant(clientID string, ttlMS, now int64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseGrant{LeaseGrant: &fsmpb.LeaseGrant{ClientId: clientID, TtlMs: ttlMS, NowUnixNano: now}}}
}

// KeepAlive builds a LeaseKeepAlive command stamped with the leader's clock.
func KeepAlive(clientID string, now int64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseKeepAlive{LeaseKeepAlive: &fsmpb.LeaseKeepAlive{ClientId: clientID, NowUnixNano: now}}}
}

// Revoke builds a LeaseRevoke command.
func Revoke(clientID string) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseRevoke{LeaseRevoke: &fsmpb.LeaseRevoke{ClientId: clientID}}}
}

// Expire builds a LeaseExpire command stamped with the leader's clock.
func Expire(clientID string, now int64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseExpire{LeaseExpire: &fsmpb.LeaseExpire{ClientId: clientID, NowUnixNano: now}}}
}

// Campaign builds a Campaign command (election entry with a published value).
func Campaign(name, clientID string, value []byte) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_Campaign{Campaign: &fsmpb.Campaign{Name: name, ClientId: clientID, Value: value}}}
}

// Proclaim builds a Proclaim command (leader updates its published value).
func Proclaim(name, clientID string, token uint64, value []byte) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_Proclaim{Proclaim: &fsmpb.Proclaim{Name: name, ClientId: clientID, FencingToken: token, Value: value}}}
}

// Resign builds a Resign command (leader relinquishes leadership).
func Resign(name, clientID string, token uint64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_Resign{Resign: &fsmpb.Resign{Name: name, ClientId: clientID, FencingToken: token}}}
}
