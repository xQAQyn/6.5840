package raft

import (
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type TrackedMutex struct {
	mu       sync.Mutex
	holderID atomic.Uint64
}

func (tm *TrackedMutex) Lock() {
	tm.mu.Lock()
	tm.holderID.Store(getGoroutineID())
}

func (tm *TrackedMutex) Unlock() {
	tm.holderID.Store(0)
	tm.mu.Unlock()
}

func (tm *TrackedMutex) IsHeldByCurrent() bool {
	return tm.holderID.Load() == getGoroutineID()
}

func (tm *TrackedMutex) AssertHeld() {
	if !tm.IsHeldByCurrent() {
		panic("lock must be held")
	}
}

func (tm *TrackedMutex) AssertNotHeld() {
	if tm.IsHeldByCurrent() {
		panic("lock must NOT be held")
	}
}

func getGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// stack 格式: "goroutine 123 [running]:..."
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseUint(fields[1], 10, 64)
	return id
}
