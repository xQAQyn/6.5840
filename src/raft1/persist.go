package raft

// Persistence: save/restore Raft's persistent state (currentTerm, voteFor, log)
// to stable storage so it can survive crashes.

import (
	"bytes"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	// 3D: snapshot boundary so the truncated log can be interpreted on restart.
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VoteFor int
	var Log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// Corrupt or old-format state: start clean.
		rf.CurrentTerm = 1
		rf.VoteFor = rf.me
		rf.Log = make([]LogEntry, 0)
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = VoteFor
		rf.Log = Log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	Logf(rf.me, "read-persist | term=%d voteFor=%d logLen=%d lastIncludedIdx=%d lastIncludedTerm=%d",
		rf.CurrentTerm, rf.VoteFor, len(rf.Log), rf.lastIncludedIndex, rf.lastIncludedTerm)
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Never go backwards, and ignore nonsensical indices. index is a logical
	// command index; it must lie within (lastIncludedIndex, lastLogIndex()].
	if index <= rf.lastIncludedIndex || index > rf.lastLogIndex() {
		return
	}

	// Term of the entry being captured by the snapshot (becomes the new
	// lastIncludedTerm). index > lastIncludedIndex guarantees getLogTerm reads
	// from the in-memory log.
	lastIncludedTerm := rf.getLogTerm(index)

	// Keep only entries strictly after `index`. Copy into a fresh backing
	// array so the discarded prefix becomes unreachable and the GC can free
	// it (no lingering references into the old slice's capacity).
	keepFrom := rf.toSliceIdx(index + 1) // first slice index to keep
	trimmed := make([]LogEntry, len(rf.Log)-keepFrom)
	copy(trimmed, rf.Log[keepFrom:])

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.Log = trimmed

	// The service has already applied everything up to `index` (it only calls
	// Snapshot after applying), so the applier should not resend it. lastApplied
	// is already >= index in normal operation; this clamp is a safety net and
	// never moves it backwards.
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	// commitIdx is never below the snapshot boundary.
	if rf.commitIdx < index {
		rf.commitIdx = index
	}

	Logf(rf.me, "snapshot | lastIncludedIdx=%d lastIncludedTerm=%d keptEntries=%d commitIdx=%d lastApplied=%d",
		rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.Log), rf.commitIdx, rf.lastApplied)
	rf.persist()
}
