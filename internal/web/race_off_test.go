//go:build !race

package web

// raceSlowdown: timing budgets of tests are multiplied by this under the race detector.
const raceSlowdown = 1
