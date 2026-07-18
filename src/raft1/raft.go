package raft

// Core types, constants, and lifecycle functions for a Raft peer.
// RPC handlers are in append_entries.go, request_vote.go;
// election logic in election.go; log replication in replication.go;
// persistence in persist.go; utilities in util.go.

import (
	"context"
	"math/rand"
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

	index := len(rf.Log) + 1
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
		commitIdx:   -1,

		electionCancelFunc: func() {},

		applyCh: applyCh,
	}
	rf.heartbeatTicker = time.NewTicker(HeartBeatInterval)
	rf.electionTimer = time.NewTimer(randomTime(ElectionTimeout, 2*ElectionTimeout))
	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.role.Load() == nil || rf.role.Load() == Candidate {
		rf.mu.Lock()
		rf.becomeFollower(rf.CurrentTerm, "init")
		rf.mu.Unlock()
	}

	// start ticker goroutine to start elections
	go rf.run()

	return rf
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
	Logf(rf.me, "become-leader | term=%d logLen=%d commitIdx=%d log=%s",
		rf.CurrentTerm, len(rf.Log), rf.commitIdx, fmtLog(rf.Log))
	rf.role.Store(Leader)

	for i := range rf.peers {
		rf.nextIdx[i] = len(rf.Log)
		rf.matchIdx[i] = -1
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
