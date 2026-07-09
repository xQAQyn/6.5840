package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&Log) != nil {
		rf.CurrentTerm = 1
		rf.VoteFor = rf.me
		rf.Log = make([]LogEntry, 0)
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = VoteFor
		rf.Log = Log
	}
	Logf(rf.me, "read-persist | term=%d voteFor=%d logLen=%d", rf.CurrentTerm, rf.VoteFor, len(rf.Log))
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

}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Logf(rf.me, "request-vote-recv | from=S%d argsTerm=%d lastLogIdx=%d lastLogTerm=%d voteFor=%d logLen=%d",
		args.CandidateId, args.Term, args.LastLogIdx, args.LastLogTerm, rf.VoteFor, len(rf.Log))

	if args.Term < rf.CurrentTerm {
		Logf(rf.me, "request-vote-reject | to=S%d reason=stale-term argsTerm=%d curTerm=%d",
			args.CandidateId, args.Term, rf.CurrentTerm)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.CurrentTerm && rf.VoteFor != -1 && rf.VoteFor != args.CandidateId {
		Logf(rf.me, "request-vote-reject | to=S%d reason=already-voted votedFor=S%d",
			args.CandidateId, rf.VoteFor)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if len(rf.Log) > 0 {
		lastLogTerm := rf.Log[len(rf.Log)-1].Term
		lastLogIdx := len(rf.Log) - 1
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIdx > args.LastLogIdx) {
			Logf(rf.me, "request-vote-reject | to=S%d reason=log-not-up-to-date myLastTerm=%d myLastIdx=%d theirLastTerm=%d theirLastIdx=%d",
				args.CandidateId, lastLogTerm, lastLogIdx, args.LastLogTerm, args.LastLogIdx)
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			return
		}
	}

	Logf(rf.me, "request-vote-grant | to=S%d", args.CandidateId)
	if args.Term > rf.CurrentTerm {
		Logf(rf.me, "request-vote-grant | to=S%d (higher term, stepping down)", args.CandidateId)
		rf.becomeFollower(args.Term, "higher-term-request-vote")
	}
	rf.VoteFor = args.CandidateId
	rf.persist()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	VLogf(rf.me, "send-request-vote | ->S%d argsTerm=%d lastLogIdx=%d lastLogTerm=%d",
		server, args.Term, args.LastLogIdx, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	VLogf(rf.me, "send-request-vote | <-S%d ok=%v replyTerm=%d voteGranted=%v",
		server, ok, reply.Term, reply.VoteGranted)
	return ok
}

type PreVoteArgs struct {
	LastLogIdx  int
	LastLogTerm int
}

type PreVoteReply struct {
	VoteGranted bool
}

func (rf *Raft) PreRequestVote(args *PreVoteArgs, reply *PreVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	VLogf(rf.me, "prevote-recv | argsLastLogIdx=%d argsLastLogTerm=%d logLen=%d",
		args.LastLogIdx, args.LastLogTerm, len(rf.Log))

	if len(rf.Log) > 0 {
		lastLogTerm := rf.Log[len(rf.Log)-1].Term
		lastLogIdx := len(rf.Log) - 1
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIdx > args.LastLogIdx) {
			VLogf(rf.me, "prevote-reject | reason=log-not-up-to-date myLastTerm=%d myLastIdx=%d theirLastTerm=%d theirLastIdx=%d",
				lastLogTerm, lastLogIdx, args.LastLogTerm, args.LastLogIdx)
			reply.VoteGranted = false
			return
		}
	}

	VLogf(rf.me, "prevote-grant")
	reply.VoteGranted = true
}

func (rf *Raft) sendPreRequestVote(server int, args *PreVoteArgs, reply *PreVoteReply) bool {
	ok := rf.peers[server].Call("Raft.PreRequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm       int
	ConflictTermLogIdx int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Logf(rf.me, "append-entries-recv | from=S%d leaderTerm=%d prevIdx=%d prevTerm=%d leaderCommit=%d nEntries=%d myRole=%v myLogLen=%d myCommitIdx=%d",
		args.LeaderId, args.Term, args.PrevLogIdx, args.PrevLogTerm, args.LeaderCommit, len(args.Entries), rf.role.Load().(Role), len(rf.Log), rf.commitIdx)
	if len(args.Entries) > 0 {
		Logf(rf.me, "append-entries-entries | %s", fmtEntries(args.Entries, args.PrevLogIdx+1))
	}

	if args.Term < rf.CurrentTerm {
		Logf(rf.me, "append-entries-reject | to=S%d reason=stale-term leaderTerm=%d curTerm=%d",
			args.LeaderId, args.Term, rf.CurrentTerm)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIdx >= 0 && (len(rf.Log) < args.PrevLogIdx+1 || rf.Log[args.PrevLogIdx].Term != args.PrevLogTerm) { // log inconsistent
		// Log EXACTLY what we have vs what they expect
		Logf(rf.me, "append-entries-conflict | prevIdx=%d prevTerm=%d myLogLen=%d",
			args.PrevLogIdx, args.PrevLogTerm, len(rf.Log))
		if args.PrevLogIdx < len(rf.Log) {
			Logf(rf.me, "append-entries-conflict | atIdx=%d myTerm=%d theirPrevTerm=%d",
				args.PrevLogIdx, rf.Log[args.PrevLogIdx].Term, args.PrevLogTerm)
		}
		reply.Term = rf.CurrentTerm
		reply.Success = false
		if len(rf.Log)-1 < args.PrevLogIdx {
			reply.ConflictTerm = -1
			reply.ConflictTermLogIdx = -1
		} else {
			reply.ConflictTerm = rf.Log[args.PrevLogIdx].Term
			reply.ConflictTermLogIdx = args.PrevLogIdx
			for reply.ConflictTermLogIdx > 0 && rf.Log[reply.ConflictTermLogIdx].Term == rf.Log[reply.ConflictTermLogIdx-1].Term {
				reply.ConflictTermLogIdx--
			}
		}
		return
	}

	// valid RPC below
	if rf.role.Load() == Candidate { // non-blocking send signal
		Logf(rf.me, "append-entries-stepdown | from=S%d (candidate stepping down)", args.LeaderId)
		select {
		case rf.electionResetCh <- args.Term:
		default:
		}
	}
	rf.resetElectionTimer()

	// Log what we're about to overwrite, if anything
	matchPoint := args.PrevLogIdx + 1
	if matchPoint < len(rf.Log) {
		Logf(rf.me, "append-entries-truncate | from=S%d keeping[0..%d] overwriting[%d..%d]=%s",
			args.LeaderId, args.PrevLogIdx, matchPoint, len(rf.Log)-1,
			fmtEntries(rf.Log[matchPoint:], matchPoint))
	}

	rf.Log = append(rf.Log[:args.PrevLogIdx+1], args.Entries...)
	rf.persist()
	if len(args.Entries) > 0 {
		Logf(rf.me, "append-entries-apply | newLogLen=%d newLog=%s", len(rf.Log), fmtLog(rf.Log))
	}

	if args.LeaderCommit > rf.commitIdx {
		newCommitIdx := min(args.LeaderCommit, len(rf.Log)-1)
		Logf(rf.me, "append-entries-commit | from=S%d oldCommitIdx=%d newCommitIdx=%d leaderCommit=%d",
			args.LeaderId, rf.commitIdx, newCommitIdx, args.LeaderCommit)
		rf.commitEntries(newCommitIdx)
	}

	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term, "higher-term-append-entries")
	}
	reply.Term = rf.CurrentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, retry bool) bool {
	VLogf(rf.me, "send-append-entries | ->S%d argsTerm=%d prevIdx=%d leaderCommit=%d nEntries=%d",
		server, args.Term, args.PrevLogIdx, args.LeaderCommit, len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if retry && !ok {
		for !ok {
			time.Sleep(QuickRetryTime)
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		}
	}
	VLogf(rf.me, "send-append-entries | <-S%d ok=%v replyTerm=%d success=%v",
		server, ok, reply.Term, reply.Success)
	return ok
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
	rf.mu.Unlock()

	Logf(rf.me, "start | idx=%d term=%d cmd=%v logLen=%d", index, term, command, len(rf.Log))
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

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()

	VLogf(rf.me, "send-heartbeat | term=%d logLen=%d commitIdx=%d", rf.CurrentTerm, len(rf.Log), rf.commitIdx)

	args := make([]*AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		arg := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   len(rf.Log) - 1,
			LeaderCommit: rf.commitIdx,
		}
		if len(rf.Log) == 0 {
			arg.PrevLogTerm = -1
		} else {
			arg.PrevLogTerm = rf.Log[len(rf.Log)-1].Term
		}

		args[i] = &arg
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, arg *AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, arg, &reply, false)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.CurrentTerm < reply.Term {
					Logf(rf.me, "step-down | from=S%d theirTerm=%d myTerm=%d reason=higher-term-heartbeat-reply",
						server, reply.Term, rf.CurrentTerm)
					rf.becomeFollower(reply.Term, "higher-term-heartbeat-reply")
				}
			}
		}(i, args[i])
	}
}

func (rf *Raft) sendNewCommand() {
	rf.mu.Lock()
	if rf.role.Load() != Leader {
		rf.mu.Unlock()
		return
	}

	args := make([]*AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		arg := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   rf.nextIdx[i] - 1,
			LeaderCommit: rf.commitIdx,
		}
		// Safety: if log was truncated (e.g., by a newer leader), reset nextIdx
		if arg.PrevLogIdx >= len(rf.Log) {
			Logf(rf.me, "replicate-trunc | to=S%d nextIdx=%d >= logLen=%d, resetting nextIdx",
				i, rf.nextIdx[i], len(rf.Log))
			rf.nextIdx[i] = len(rf.Log)
			arg.PrevLogIdx = rf.nextIdx[i] - 1
		}
		if arg.PrevLogIdx < 0 {
			arg.PrevLogTerm = -1
		} else {
			arg.PrevLogTerm = rf.Log[arg.PrevLogIdx].Term
		}
		if len(rf.Log)-1 > arg.PrevLogIdx {
			entries := rf.Log[rf.nextIdx[i]:len(rf.Log)]
			arg.Entries = make([]LogEntry, len(entries))
			copy(arg.Entries, entries)
		}

		args[i] = &arg
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int, arg *AppendEntriesArgs) {
			if len(arg.Entries) > 0 {
				Logf(rf.me, "replicate | to=S%d prevIdx=%d nEntries=%d entries=%s",
					server, arg.PrevLogIdx, len(arg.Entries), fmtEntries(arg.Entries, arg.PrevLogIdx+1))
			} else {
				VLogf(rf.me, "heartbeat | to=S%d prevIdx=%d", server, arg.PrevLogIdx)
			}

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, arg, &reply, true)
			sendEndIdx := arg.PrevLogIdx + len(arg.Entries)
			if !reply.Success {
				rf.mu.Lock()
				if rf.CurrentTerm < reply.Term {
					Logf(rf.me, "step-down | from=S%d theirTerm=%d myTerm=%d reason=higher-term-replicate-reply",
						server, reply.Term, rf.CurrentTerm)
					rf.becomeFollower(reply.Term, "higher-term-replicate-reply")
					rf.mu.Unlock()
					return
				} else { // log inconsistency, keep retry until success
					Logf(rf.me, "replicate-retry | to=S%d prevIdx=%d conflictTerm=%d conflictIdx=%d",
						server, rf.nextIdx[server], reply.ConflictTerm, reply.ConflictTermLogIdx)
					rf.mu.Unlock()
					for !reply.Success {

						rf.mu.Lock()
						if reply.ConflictTerm != -1 {
							Logf(rf.me, "replicate-retry | to=S%d quick-decrease nextIdx[%d]: %d -> %d",
								server, server, rf.nextIdx[server], reply.ConflictTermLogIdx)
							rf.nextIdx[server] = reply.ConflictTermLogIdx
						} else if rf.nextIdx[server] > 0 {
							Logf(rf.me, "replicate-retry | to=S%d slow-decrease nextIdx[%d]: %d -> %d",
								server, server, rf.nextIdx[server], rf.nextIdx[server]-1)
							rf.nextIdx[server]--
						}

						if rf.role.Load() != Leader {
							rf.mu.Unlock()
							return
						}
						arg.PrevLogIdx = rf.nextIdx[server] - 1
						arg.PrevLogTerm = -1
						if arg.PrevLogIdx >= 0 {
							arg.PrevLogTerm = rf.Log[arg.PrevLogIdx].Term
						}
						if len(rf.Log)-1 > arg.PrevLogIdx {
							entries := rf.Log[rf.nextIdx[server]:len(rf.Log)]
							arg.Entries = make([]LogEntry, len(entries))
							copy(arg.Entries, entries)
						}
						rf.mu.Unlock()

						if len(arg.Entries) > 0 {
							Logf(rf.me, "replicate-retry-entries | to=S%d entries=%s",
								server, fmtEntries(arg.Entries, arg.PrevLogIdx+1))
						}
						reply = AppendEntriesReply{}
						rf.sendAppendEntries(server, arg, &reply, true)
						sendEndIdx = arg.PrevLogIdx + len(arg.Entries)

						rf.mu.Lock()
						if rf.CurrentTerm < reply.Term {
							Logf(rf.me, "step-down | from=S%d theirTerm=%d myTerm=%d reason=higher-term-retry-reply",
								server, reply.Term, rf.CurrentTerm)
							rf.becomeFollower(reply.Term, "higher-term-retry-reply")
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					}
				}
			}

			// already success, update
			rf.mu.Lock()
			rf.nextIdx[server] = sendEndIdx + 1
			rf.matchIdx[server] = sendEndIdx
			Logf(rf.me, "replicate-ok | to=S%d nextIdx=%d matchIdx=%d",
				server, rf.nextIdx[server], rf.matchIdx[server])
			rf.triggerCommit()
			rf.mu.Unlock()
		}(i, args[i])
	}
}

func (rf *Raft) triggerCommit() {
	rf.mu.AssertHeld()

	for commitIdx := len(rf.Log) - 1; commitIdx > rf.commitIdx; commitIdx-- {
		cnt := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIdx[i] >= commitIdx {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			if rf.Log[commitIdx].Term == rf.CurrentTerm {
				Logf(rf.me, "trigger-commit | checkIdx=%d entryTerm=%d curTerm=%d votes=%d/%d matchIdx=%v",
					commitIdx, rf.Log[commitIdx].Term, rf.CurrentTerm, cnt, len(rf.peers), rf.matchIdx)
				rf.commitEntries(commitIdx)
			} else {
				Logf(rf.me, "trigger-commit-skip | idx=%d entryTerm=%d curTerm=%d (will not commit from past term)",
					commitIdx, rf.Log[commitIdx].Term, rf.CurrentTerm)
			}
			break
		}
	}
}

func (rf *Raft) commitEntries(newCommitIdx int) {
	Logf(rf.me, "commit | from=%d to=%d logLen=%d", rf.commitIdx+1, newCommitIdx, len(rf.Log))
	for i := rf.commitIdx + 1; i <= newCommitIdx; i++ {
		Logf(rf.me, "commit-entry | idx=%d term=%d cmd=%v", i, rf.Log[i].Term, rf.Log[i].Cmd)
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[i].Cmd,
			CommandIndex: i + 1,
		}
	}
	rf.commitIdx = newCommitIdx
}

func (rf *Raft) startPreVote() bool {
	rf.mu.Lock()
	Logf(rf.me, "prevote-start | term=%d logLen=%d lastLogTerm=%d",
		rf.CurrentTerm, len(rf.Log), func() int {
			if len(rf.Log) > 0 {
				return rf.Log[len(rf.Log)-1].Term
			}
			return -1
		}())

	args := make([]*PreVoteArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		arg := PreVoteArgs{
			LastLogIdx:  len(rf.Log) - 1,
			LastLogTerm: -1,
		}
		if len(rf.Log) > 0 {
			arg.LastLogTerm = rf.Log[len(rf.Log)-1].Term
		}
		args[i] = &arg
	}

	rf.mu.Unlock()

	var preVoteApproveCnt atomic.Int32
	var preVoteRejectCnt atomic.Int32
	preVoteApproveCnt.Store(1)
	preVoteRejectCnt.Store(0)

	winC := make(chan struct{}, 1)
	loseC := make(chan struct{}, 1)
	var firstSig sync.Once

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, arg *PreVoteArgs) {
			reply := PreVoteReply{}
			ok := rf.sendPreRequestVote(server, arg, &reply)
			if ok && reply.VoteGranted {
				VLogf(rf.me, "prevote-granted-by | S%d", server)
				preVoteApproveCnt.Add(1)
				if int(preVoteApproveCnt.Load()) > len(rf.peers)/2 {
					firstSig.Do(func() {
						select {
						case winC <- struct{}{}:
						default:
						}
					})
				}
			} else {
				VLogf(rf.me, "prevote-rejected-by | S%d ok=%v grant=%v", server, ok, reply.VoteGranted)
				preVoteRejectCnt.Add(1)
				if int(preVoteRejectCnt.Load()) > len(rf.peers)/2 {
					firstSig.Do(func() {
						select {
						case loseC <- struct{}{}:
						default:
						}
					})
				}
			}
		}(i, args[i])
	}

	select {
	case <-winC:
		Logf(rf.me, "prevote-won | approveCnt=%d/%d", preVoteApproveCnt.Load(), len(rf.peers))
		return true
	case <-loseC:
		Logf(rf.me, "prevote-lost | rejectCnt=%d/%d", preVoteRejectCnt.Load(), len(rf.peers))
		return false
	}
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	// pre request vote
	if !rf.startPreVote() {
		return
	}

	rf.mu.Lock()
	rf.role.Store(Candidate)
	rf.raiseTerm(rf.CurrentTerm + 1)
	rf.VoteFor = rf.me
	rf.persist()
	rf.voteCnt = 1

	Logf(rf.me, "election-start | term=%d logLen=%d lastLogTerm=%d lastLogIdx=%d",
		rf.CurrentTerm, len(rf.Log), func() int {
			if len(rf.Log) > 0 {
				return rf.Log[len(rf.Log)-1].Term
			}
			return -1
		}(), len(rf.Log)-1)

	args := make([]*RequestVoteArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		arg := RequestVoteArgs{
			Term:        rf.CurrentTerm,
			CandidateId: rf.me,
			LastLogIdx:  len(rf.Log) - 1,
			LastLogTerm: -1,
		}
		if len(rf.Log) > 0 {
			arg.LastLogTerm = rf.Log[len(rf.Log)-1].Term
		}
		args[i] = &arg
	}

	ctx, cancel := context.WithCancel(context.Background())
	rf.electionCancelFunc = cancel
	rf.mu.Unlock()

	winC := make(chan struct{})
	var firstWin sync.Once
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, arg *RequestVoteArgs, ctx context.Context) {
			done := make(chan RequestVoteReply, 1)

			go func() {
				defer func() {
					select {
					case done <- RequestVoteReply{VoteGranted: false}:
					default:
					}
				}()

				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, arg, &reply)
				for !ok {
					select {
					case <-ctx.Done():
						return
					case <-time.After(SleepLength):
						ok = rf.sendRequestVote(i, arg, &reply)
					}
				}

				done <- reply
			}()

			select {
			case reply := <-done:
				VLogf(rf.me, "vote-response | from=S%d granted=%v term=%d", server, reply.VoteGranted, reply.Term)
				rf.mu.Lock()
				if reply.VoteGranted {
					rf.voteCnt++
					Logf(rf.me, "vote-granted-by | S%d voteCnt=%d/%d", server, rf.voteCnt, len(rf.peers))
					if rf.voteCnt > len(rf.peers)/2 {
						firstWin.Do(func() {
							Logf(rf.me, "election-won | votes=%d/%d", rf.voteCnt, len(rf.peers))
							select {
							case winC <- struct{}{}:
							default:
							}
						})
					}
				} else if reply.Term > rf.CurrentTerm { // stop election
					Logf(rf.me, "election-abort | S%d replied with higher term=%d > curTerm=%d",
						server, reply.Term, rf.CurrentTerm)
					rf.becomeFollower(reply.Term, "higher-term-vote-reply")
					rf.electionCancelFunc()
				} else {
					VLogf(rf.me, "vote-denied-by | S%d replyTerm=%d", server, reply.Term)
				}
				rf.mu.Unlock()
			case <-ctx.Done():
				return
			}

		}(i, args[i], ctx)
	}
	select {
	case <-winC:
		Logf(rf.me, "election-won | becoming leader")
		rf.mu.Lock()
		rf.becomeLeader()
		rf.mu.Unlock()
		rf.sendNewCommand()
	case <-rf.electionTimer.C:
		Logf(rf.me, "election-timeout | restarting election")
		rf.electionCancelFunc()
		rf.startElection()
	case term := <-rf.electionResetCh:
		Logf(rf.me, "election-reset | higher term=%d, becoming follower", term)
		rf.electionCancelFunc()
		rf.becomeFollower(term, "election-reset-by-leader")
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
