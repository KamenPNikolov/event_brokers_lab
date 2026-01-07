package common

import "time"

func Backoff(attempt int) time.Duration {
	// 0 -> 0.5s, 1 -> 1s, 2 -> 2s, ... capped
	d := time.Duration(1<<attempt) * 500 * time.Millisecond
	if d > 20*time.Second {
		return 20 * time.Second
	}
	return d
}
