package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"strand/internal/tunnel"
)

func newTunnelCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "tunnel", Short: "Create verified SSH tunnels for private database endpoints"}
	cmd.AddCommand(newTunnelOpenCmd())
	return cmd
}

func newTunnelOpenCmd() *cobra.Command {
	var sshAddress, sshUser, identityFile, hostKeysFile, localAddress, remoteAddress string
	cmd := &cobra.Command{
		Use:   "open",
		Short: "Forward a local TCP port through SSH to a private host",
		Long:  "Open a loopback-only local port forward. SSH host keys are checked against known_hosts; unknown hosts are rejected.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			if hostKeysFile == "" {
				var err error
				hostKeysFile, err = tunnel.DefaultKnownHosts()
				if err != nil {
					return fmt.Errorf("find default known_hosts file: %w", err)
				}
			}
			forward, err := tunnel.Start(ctx, tunnel.Config{
				SSHAddress: sshAddress, SSHUser: sshUser, IdentityFile: identityFile,
				HostKeysFile: hostKeysFile, LocalAddress: localAddress, Remote: remoteAddress,
			})
			if err != nil {
				return err
			}
			defer forward.Close()
			fmt.Fprintf(cmd.OutOrStdout(), "SSH tunnel listening on %s -> %s via %s\n", forward.Address(), remoteAddress, sshAddress)
			<-ctx.Done()
			return nil
		},
	}
	cmd.Flags().StringVar(&sshAddress, "ssh", "", "SSH server host[:port]")
	cmd.Flags().StringVar(&sshUser, "user", "", "SSH username")
	cmd.Flags().StringVar(&identityFile, "identity-file", "", "unencrypted SSH private key file (or use SSH_AUTH_SOCK)")
	cmd.Flags().StringVar(&hostKeysFile, "known-hosts", "", "known_hosts file (default ~/.ssh/known_hosts)")
	cmd.Flags().StringVar(&localAddress, "listen", "127.0.0.1:0", "local listen address; port 0 selects a free port")
	cmd.Flags().StringVar(&remoteAddress, "remote", "", "remote host:port as seen from the SSH server")
	_ = cmd.MarkFlagRequired("ssh")
	_ = cmd.MarkFlagRequired("user")
	_ = cmd.MarkFlagRequired("remote")
	return cmd
}
