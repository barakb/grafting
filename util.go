package go_rafting

import "time"

const minDuration time.Duration = -1 << 63

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func durationUntil(t time.Time) time.Duration {
	res := t.Sub(time.Now())
	if res == minDuration {
		return 0
	}
	return res
}
