package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"golang.org/x/crypto/ssh"
)

// Dialer routes driver connections, including replica-set discovery, through
// one verified SSH session. A failed session can be reopened on the next dial.
type Dialer struct {
	ctx    context.Context
	cancel context.CancelFunc
	load   func(context.Context) (Config, error)
	gate   chan struct{}
	client *ssh.Client // protected by gate
}

func NewDialer(ctx context.Context, load func(context.Context) (Config, error)) *Dialer {
	ctx, cancel := context.WithCancel(ctx)
	d := &Dialer{ctx: ctx, cancel: cancel, load: load, gate: make(chan struct{}, 1)}
	context.AfterFunc(ctx, func() {
		d.gate <- struct{}{}
		if d.client != nil {
			_ = d.client.Close()
			d.client = nil
		}
		<-d.gate
	})
	return d
}

func (d *Dialer) Close() { d.cancel() }

func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if err := validateTCPAddress(addr); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(d.ctx, cancel)
	defer stop()
	client, err := d.session(ctx)
	if err != nil {
		return nil, err
	}
	remote, err := client.DialContext(ctx, network, addr)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			_ = client.Close()
		}
		return nil, err
	}
	// SSH channels do not implement deadlines. net.Pipe supplies them to
	// the driver and carries bytes over the channel without a TCP listener.
	local, bridge := net.Pipe()
	var once sync.Once
	closeBridge := func() { once.Do(func() { _ = bridge.Close(); _ = remote.Close() }) }
	go func() { _, _ = io.Copy(bridge, remote); closeBridge() }()
	go func() { _, _ = io.Copy(remote, bridge); closeBridge() }()
	return local, nil
}

func (d *Dialer) session(ctx context.Context) (*ssh.Client, error) {
	select {
	case d.gate <- struct{}{}:
		defer func() { <-d.gate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := d.ctx.Err(); err != nil {
		return nil, err
	}
	if d.client != nil {
		return d.client, nil
	}
	cfg, err := d.load(ctx)
	if err != nil {
		return nil, err
	}
	defer cfg.ClearCredentials()
	if err := cfg.PrepareServiceTrust(); err != nil {
		return nil, err
	}
	client, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	d.client = client
	go func() {
		_ = client.Wait()
		d.gate <- struct{}{}
		if d.client == client {
			d.client = nil
		}
		<-d.gate
	}()
	return client, nil
}
