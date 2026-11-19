//go:build race

package web

// raceSlowdown: timing budgets of tests are multiplied by this under the race detector (~5-10x slower).
const raceSlowdown = 8
