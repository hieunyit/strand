//go:build !linux

package web

import (
	"errors"
	"syscall"
)

func signalJobProcess(int, string, string, string, syscall.Signal) error {
	return errors.New("verified job process signaling requires Linux pidfds")
}
