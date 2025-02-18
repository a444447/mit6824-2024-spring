package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

const baseElecInterval int = 450
const baseHeartbeatInterval int = 101
const baseCommitCheckInterval int = 100

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func GetRandomElecInterval() time.Duration {
	return time.Duration(baseElecInterval+rand.Intn(150)) * time.Millisecond
}

func GetStableHeartbeatInterval() time.Duration {
	return time.Duration(baseHeartbeatInterval) * time.Millisecond
}

func GetRandomElectTimeOut(rd *rand.Rand) int {
	plusMs := int(rd.Float64() * 500.0)

	return plusMs + ElectTimeOutBase
}
