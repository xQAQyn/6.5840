package raft

// Core types, constants, and lifecycle functions for a Raft peer.
// RPC handlers are in append_entries.go, request_vote.go;
// election logic in election.go; log replication in replication.go;
// persistence in persist.go; utilities in util.go.

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

const (
	Follower Role = iota
	Leader
	Candidate
)

const (
	ElectionTimeout   = 1000 * time.Millisecond
	HeartBeatInterval = 200 * time.Millisecond
	SleepLength       = 80 * time.Millisecond
	QuickRetryTime    = 15 * time.Millisecond
	// PreVoteRPCTimeout caps how long sendPreRequestVote waits for a reply.
	// PreVote is only an optimization; a peer whose link is partitioned makes
	// labrpc delay the failure reply by up to LONGDELAY (~7s). Blocking that
	// long would pin startPreVote -- and the run() loop driving elections --
	// far past the election timeout, so a healed partition can't re-elect in
	// time. Keep it well under ElectionTimeout so unreachable peers are simply
	// counted as no-votes and elections retry on the timer cadence.
	PreVoteRPCTimeout = ElectionTimeout / 2
)

type LogEntry struct {
	Term int
	Cmd  any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        TrackedMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        atomic.Value
	CurrentTerm int
	VoteFor     int
	Log         []LogEntry

	commitIdx   int
	lastApplied int
	applyCh     chan raftapi.ApplyMsg

	// Snapshot (3D). lastIncludedIndex is the logical index of the last log
	// entry captured by the snapshot (0 = no snapshot yet, so the first real
	// entry is logical index 1 -- matching the pre-snapshot behavior).
	// lastIncludedTerm is that entry's term. snapshot holds the
	// service-supplied snapshot bytes. rf.Log stores only the entries AFTER
	// the snapshot: rf.Log[i] has logical index lastIncludedIndex + 1 + i.
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	// applyCond wakes the applier goroutine when commitIdx advances or a
	// snapshot is installed. The applier is the single sender to applyCh, so
	// it sends in strict index order and -- crucially -- performs the channel
	// send WITHOUT holding rf.mu (the service may call back into Snapshot()
	// while we send, which would otherwise deadlock).
	applyCond *sync.Cond

	electionCancelFunc context.CancelFunc
	electionTimer      *time.Timer
	electionResetCh    chan int
	voteCnt            int

	heartbeatTicker *time.Ticker

	// volatile state on leader
	nextIdx  []int
	matchIdx []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.CurrentTerm, rf.role.Load() == Leader
	return term, isLeader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	if rf.role.Load() != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	index := rf.lastLogIndex() + 1
	term := rf.CurrentTerm
	isLeader := true
	entry := LogEntry{
		Term: rf.CurrentTerm,
		Cmd:  command,
	}
	rf.Log = append(rf.Log, entry)
	rf.persist()
	Logf(rf.me, "start | idx=%d term=%d cmd=%v logLen=%d", index, term, command, len(rf.Log))
	rf.mu.Unlock()

	rf.sendNewCommand()

	return index, term, isLeader
}

func (rf *Raft) run() {
	VLogf(rf.me, "run-loop-started")
	for true {
		select {
		case <-rf.heartbeatTicker.C:
			if rf.role.Load() == Leader {
				VLogf(rf.me, "heartbeat-tick")
				rf.sendNewCommand()
			}
		case <-rf.electionTimer.C:
			if rf.role.Load() != Leader {
				VLogf(rf.me, "election-timer-fired")
				rf.startElection()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		CurrentTerm: 1,
		VoteFor:     -1,
		Log:         make([]LogEntry, 0),

		// commitIdx / lastApplied / lastIncludedIndex default to 0: with no
		// snapshot, logical index 0 is "nothing committed/applied yet".
		commitIdx:         0,
		lastApplied:       0,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,

		electionCancelFunc: func() {},

		applyCh: applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.heartbeatTicker = time.NewTicker(HeartBeatInterval)
	rf.electionTimer = time.NewTimer(randomTime(ElectionTimeout, 2*ElectionTimeout))
	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// The persister also holds the service snapshot; keep a copy so we can
	// re-send it via InstallSnapshot and re-include it in every Save().
	rf.snapshot = persister.ReadSnapshot()
	// On restart the service restores the snapshot itself (see newRfsrv), so
	// the applier must not resend it: start lastApplied at the snapshot
	// boundary. commitIdx is never below the snapshot boundary.
	rf.lastApplied = rf.lastIncludedIndex
	if rf.commitIdx < rf.lastIncludedIndex {
		rf.commitIdx = rf.lastIncludedIndex
	}

	if rf.role.Load() == nil || rf.role.Load() == Candidate {
		rf.mu.Lock()
		rf.becomeFollower(rf.CurrentTerm, "init")
		rf.mu.Unlock()
	}

	// start ticker goroutine to start elections
	go rf.run()
	// start the applier goroutine that drains committed entries / snapshots
	// onto applyCh (single sender, sends outside the lock)
	go rf.applier()

	return rf
}

// applier is the single goroutine that sends ApplyMsgs on applyCh. It gathers
// what needs applying under rf.mu, then releases the lock before sending --
// the service may call back into rf.Snapshot() while we block on the channel
// send, and doing that under rf.mu would deadlock. Being the sole sender also
// guarantees commands are delivered in strict index order.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		// Wait until there is something to apply: either a committed entry
		// beyond lastApplied, or an installed snapshot ahead of lastApplied.
		for rf.lastApplied == rf.commitIdx {
			rf.applyCond.Wait()
		}
		var msgs []raftapi.ApplyMsg
		if rf.lastApplied < rf.lastIncludedIndex {
			// An installed snapshot the service has not seen yet. Send it first
			// so the command indices that follow line up. This only ever
			// advances the service's state forward (never backwards) because we
			// only install snapshots with index > our previous lastIncludedIndex.
			msgs = append(msgs, raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			})
			rf.lastApplied = rf.lastIncludedIndex
		}
		for idx := rf.lastApplied + 1; idx <= rf.commitIdx; idx++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.getEntry(idx).Cmd,
				CommandIndex: idx,
			})
		}
		rf.lastApplied = rf.commitIdx
		rf.mu.Unlock()

		for _, m := range msgs {
			rf.applyCh <- m
		}
	}
}

func (rf *Raft) raiseTerm(term int) {
	rf.mu.AssertHeld() // must hold lock
	Logf(rf.me, "raise-term | %d -> %d", rf.CurrentTerm, term)
	rf.CurrentTerm = term
	rf.VoteFor = -1
	rf.persist()
}

func (rf *Raft) becomeFollower(term int, reason string) {
	rf.mu.AssertHeld() // must hold lock
	Logf(rf.me, "become-follower | newTerm=%d curTerm=%d reason=%s", term, rf.CurrentTerm, reason)
	if rf.CurrentTerm < term {
		rf.raiseTerm(term)
	}

	rf.role.Store(Follower)
	rf.electionCancelFunc() // remove existing election process
	rf.resetElectionTimer() // start election timer
}

func (rf *Raft) becomeLeader() {
	rf.mu.AssertHeld() // must hold lock
	Logf(rf.me, "become-leader | term=%d lastLogIdx=%d commitIdx=%d lastIncludedIdx=%d log=%s",
		rf.CurrentTerm, rf.lastLogIndex(), rf.commitIdx, rf.lastIncludedIndex, fmtLog(rf.Log))
	rf.role.Store(Leader)

	// nextIdx/matchIdx are logical indices. Optimistically start each follower
	// at the end of our log; back off on conflict. matchIdx floors at the
	// snapshot boundary (everything in the snapshot is assumed replicated,
	// which is safe -- it never helps commit any future entry until the
	// follower actually catches up past it).
	for i := range rf.peers {
		rf.nextIdx[i] = rf.lastLogIndex() + 1
		rf.matchIdx[i] = rf.lastIncludedIndex
	}
}

func (rf *Raft) resetElectionTimer() {
	// safe reset timer
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	d := randomTime(ElectionTimeout, 2*ElectionTimeout)
	VLogf(rf.me, "reset-election-timer | timeout=%v", d)
	rf.electionTimer.Reset(d)
}

func randomTime(minimal, maximum time.Duration) time.Duration {
	if minimal > maximum {
		minimal, maximum = maximum, minimal
	}

	diff := maximum - minimal
	if diff == 0 {
		return minimal
	}

	randomNanos := rand.Int63n(diff.Nanoseconds() + 1)
	return minimal + time.Duration(randomNanos)
}
