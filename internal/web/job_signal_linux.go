package web

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

// Open the descriptor before validating the launch ID. If the PID changes hands
// during validation, the descriptor still points at the old process.
func signalJobProcess(pid int, id, bin, logPath string, sig syscall.Signal) error {
	if pid <= 0 {
		return errJobNotRunning
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return fmt.Errorf("open process descriptor: %w", err)
	}
	defer unix.Close(fd)
	if !jobProcessMatches(pid, id, bin, logPath) {
		if err := unix.PidfdSendSignal(fd, 0, nil, 0); errors.Is(err, unix.ESRCH) {
			return err
		}
		return errJobNotRunning
	}
	err = unix.PidfdSendSignal(fd, sig, nil, unix.PIDFD_SIGNAL_PROCESS_GROUP)
	if errors.Is(err, unix.EINVAL) {
		// Older Linux kernels support signaling the verified leader but not the
		// process group through a pidfd.
		err = unix.PidfdSendSignal(fd, sig, nil, 0)
	}
	return err
}
