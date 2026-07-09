package raft

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Debugging
const Debug = true   // Master switch: false = no debug output
const Verbose = true // Extra-detailed tracing (heartbeats, timer resets, RPC send/recv)

// DPrintf is the original debug printf. Kept for backward compatibility.
// New code should use Logf/VLogf instead.
func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// Logf is the primary structured logging function for INFO-level events.
// me is the Raft peer ID. It is safe to call without holding any lock.
func Logf(me int, format string, a ...interface{}) {
	if Debug {
		// gid := getGoroutineID()
		prefix := fmt.Sprintf("[R%d] ", me)
		log.Printf(prefix+format, a...)
	}
}

// VLogf is verbose-level logging for TRACE events (heartbeats, timer resets, etc.).
// Suppressed when Verbose is false.
func VLogf(me int, format string, a ...interface{}) {
	if Verbose {
		Logf(me, format, a...)
	}
}

// fmtEntry formats a single log entry with its absolute index.
func fmtEntry(idx int, e LogEntry) string {
	return fmt.Sprintf("i%d:t%d", idx, e.Term)
}

// fmtLog formats the full log as a compact string for logging.
func fmtLog(log []LogEntry) string {
	parts := make([]string, len(log))
	for i, e := range log {
		parts[i] = fmtEntry(i, e)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// fmtEntries formats a slice of log entries with their absolute indices.
func fmtEntries(entries []LogEntry, startIdx int) string {
	parts := make([]string, len(entries))
	for i, e := range entries {
		parts[i] = fmtEntry(startIdx+i, e)
	}
	return "[" + strings.Join(parts, " ") + "]"
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
