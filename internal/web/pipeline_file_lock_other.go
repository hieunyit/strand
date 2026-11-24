//go:build !linux

package web

func lockPipelineFiles(string) (func(), error) { return func() {}, nil }
