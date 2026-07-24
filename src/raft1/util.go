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
const Debug = false  // Master switch: false = no debug output
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

// --- Snapshot-aware log indexing (logical indices) ---
//
// rf.Log holds only the entries AFTER the snapshot. The entry rf.Log[i] has
// logical index  lastIncludedIndex + 1 + i.  lastIncludedIndex == 0 means no
// snapshot has been taken yet (the first real entry is logical index 1), which
// reproduces the pre-snapshot behavior exactly so 3A/3B/3C are unaffected.
//
// All methods below assume the caller holds rf.mu (they read rf.Log /
// lastIncludedIndex, which are protected by it).

// lastLogIndex returns the logical index of the last log entry. When the
// in-memory log is empty this is lastIncludedIndex (the snapshot's last entry).
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.Log)
}

// lastLogTerm returns the term of the last log entry, or lastIncludedTerm
// when the in-memory log is empty (the snapshot's last entry).
func (rf *Raft) lastLogTerm() int {
	if len(rf.Log) > 0 {
		return rf.Log[len(rf.Log)-1].Term
	}
	return rf.lastIncludedTerm
}

// toSliceIdx converts a logical log index into an index into rf.Log.
// It returns a value < 0 when the index falls inside the snapshot.
func (rf *Raft) toSliceIdx(logIdx int) int {
	return logIdx - rf.lastIncludedIndex - 1
}

// getLogTerm returns the term of the log entry at logical index logIdx.
// logIdx == lastIncludedIndex yields lastIncludedTerm (the snapshot boundary).
// The caller must ensure logIdx is within [lastIncludedIndex, lastLogIndex()].
func (rf *Raft) getLogTerm(logIdx int) int {
	if logIdx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.Log[rf.toSliceIdx(logIdx)].Term
}

// getEntry returns the log entry at logical index logIdx (> lastIncludedIndex).
func (rf *Raft) getEntry(logIdx int) LogEntry {
	return rf.Log[rf.toSliceIdx(logIdx)]
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
