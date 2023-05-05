package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

// const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func getRandomTimeout() time.Duration {
	ms := rand.Int63() % 300

	return time.Duration(100+ms) * time.Millisecond
}
