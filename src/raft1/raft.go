package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	currentTerm int
	voteFor     int
	log         []LogEntry

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
	term, isLeader := rf.currentTerm, rf.role.Load() == Leader
	// DPrintf("[Raft %d] GetState: term=%d isLeader=%v", rf.me, term, isLeader)
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

	DPrintf("[Raft %d] RequestVote recv from S%d: args{Term=%d LastLogIdx=%d LastLogTerm=%d} curTerm=%d voteFor=%d logLen=%d",
		rf.me, args.CandidateId, args.Term, args.LastLogIdx, args.LastLogTerm, rf.currentTerm, rf.voteFor, len(rf.log))

	if args.Term < rf.currentTerm {
		DPrintf("[Raft %d] RequestVote -> reject S%d (candidate term=%d < curTerm=%d)",
			rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		DPrintf("[Raft %d] RequestVote -> reject S%d (already voted for S%d in term=%d)",
			rf.me, args.CandidateId, rf.voteFor, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if len(rf.log) > 0 {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		lastLogIdx := len(rf.log) - 1
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIdx > args.LastLogIdx) {
			DPrintf("[Raft %d] RequestVote -> reject S%d (log not up-to-date: my lastTerm=%d lastIdx=%d, candidate lastTerm=%d lastIdx=%d)",
				rf.me, args.CandidateId, lastLogTerm, lastLogIdx, args.LastLogTerm, args.LastLogIdx)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}

	DPrintf("[Raft %d] RequestVote -> grant to S%d", rf.me, args.CandidateId)
	if args.Term > rf.currentTerm {
		DPrintf("[Raft %d] RequestVote -> grant (higher term) to S%d, become follower term=%d", rf.me, args.CandidateId, args.Term)
		rf.becomeFollower(args.Term)
	}
	rf.voteFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[Raft %d] sendRequestVote -> S%d: args{Term=%d LastLogIdx=%d LastLogTerm=%d}", rf.me, server, args.Term, args.LastLogIdx, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("[Raft %d] sendRequestVote <- S%d: ok=%v reply{Term=%d VoteGranted=%v}", rf.me, server, ok, reply.Term, reply.VoteGranted)
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
	DPrintf("[Raft %d] PreRequestVote recv: args{LastLogIdx=%d LastLogTerm=%d} curTerm=%d logLen=%d",
		rf.me, args.LastLogIdx, args.LastLogTerm, rf.currentTerm, len(rf.log))

	if len(rf.log) > 0 {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		lastLogIdx := len(rf.log) - 1
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIdx > args.LastLogIdx) {
			DPrintf("[Raft %d] PreRequestVote -> reject (log not up-to-date: my lastTerm=%d lastIdx=%d, args lastTerm=%d lastIdx=%d)",
				rf.me, lastLogTerm, lastLogIdx, args.LastLogTerm, args.LastLogIdx)
			reply.VoteGranted = false
			return
		}
	}

	DPrintf("[Raft %d] PreRequestVote -> grant", rf.me)
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

	DPrintf("[Raft %d] AppendEntries recv from S%d: args{Term=%d PrevLogIdx=%d PrevLogTerm=%d LeaderCommit=%d Entries=%d} curTerm=%d role=%v",
		rf.me, args.LeaderId, args.Term, args.PrevLogIdx, args.PrevLogTerm, args.LeaderCommit, len(args.Entries), rf.currentTerm, rf.role.Load().(Role))

	if args.Term < rf.currentTerm {
		DPrintf("[Raft %d] AppendEntries -> reject S%d (args.Term=%d < curTerm=%d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIdx > 0 && (len(rf.log) < args.PrevLogIdx+1 || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm) { // log inconsistent
		DPrintf("[Raft %d] AppendEntries -> reject S%d (log inconsistent)", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		if len(rf.log)-1 < args.PrevLogIdx {
			reply.Term = -1
			reply.ConflictTermLogIdx = -1
		} else {
			reply.Term = rf.log[args.PrevLogIdx].Term
			reply.ConflictTermLogIdx = args.PrevLogIdx
			for reply.ConflictTermLogIdx > 0 && rf.log[reply.ConflictTermLogIdx].Term == rf.log[reply.ConflictTermLogIdx-1].Term {
				reply.ConflictTermLogIdx--
			}
		}
		return
	}

	// valid RPC below
	if rf.role.Load() == Candidate { // non-blocking send signal
		DPrintf("[Raft %d] AppendEntries from S%d: step down from candidate, reset election", rf.me, args.LeaderId)
		select {
		case rf.electionResetCh <- args.Term:
		default:
		}
	}
	rf.resetElectionTimer()

	rf.log = append(rf.log[:args.PrevLogIdx+1], args.Entries...)
	if args.LeaderCommit > rf.commitIdx {
		newCommitIdx := min(args.LeaderCommit, len(rf.log)-1)
		rf.commitEntries(newCommitIdx)
	}

	if args.Term > rf.currentTerm {
		// DPrintf("[Raft %d] AppendEntries -> raise term %d -> %d", rf.me, rf.currentTerm, args.Term)
		rf.becomeFollower(args.Term)
	}
	// DPrintf("[Raft %d] AppendEntries -> success to S%d", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, retry bool) bool {
	DPrintf("[Raft %d] sendAppendEntries -> S%d: args{Term=%d PrevLogIdx=%d LeaderCommit=%d Entries=%d}", rf.me, server, args.Term, args.PrevLogIdx, args.LeaderCommit, len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if retry && !ok {
		// DPrintf("[Raft %d] sendAppendEntries -> S%d: RPC failed, retrying...", rf.me, server)
		for !ok {
			time.Sleep(QuickRetryTime)
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		}
	}
	DPrintf("[Raft %d] sendAppendEntries <- S%d: ok=%v reply{Term=%d Success=%v}", rf.me, server, ok, reply.Term, reply.Success)
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

	if rf.role.Load() != Leader {
		return -1, -1, false
		// DPrintf("[Raft %d] Start called but folloer", rf.me)
	}

	rf.mu.Lock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := true
	entry := LogEntry{
		Term: rf.currentTerm,
		Cmd:  command,
	}
	rf.log = append(rf.log, entry)
	rf.mu.Unlock()

	DPrintf("[Raft %d] Start called: command=%v", rf.me, command)
	rf.sendNewCommand()

	return index, term, isLeader
}

func (rf *Raft) run() {
	// DPrintf("[Raft %d] run loop started", rf.me)
	for true {
		select {
		case <-rf.heartbeatTicker.C:
			if rf.role.Load() == Leader {
				// DPrintf("[Raft %d] heartbeat ticker fired, sending heartbeat", rf.me)
				rf.sendNewCommand()
			}
		case <-rf.electionTimer.C:
			if rf.role.Load() != Leader {
				// DPrintf("[Raft %d] election timer fired, starting election", rf.me)
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()

	DPrintf("[Raft %d] sendHeartBeat: term=%d logLen=%d commitIdx=%d", rf.me, rf.currentTerm, len(rf.log), rf.commitIdx)

	args := make([]*AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		arg := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   len(rf.log) - 1,
			LeaderCommit: rf.commitIdx,
		}
		if len(rf.log) == 0 {
			arg.PrevLogTerm = -1
		} else {
			arg.PrevLogTerm = rf.log[len(rf.log)-1].Term
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
				if rf.currentTerm < reply.Term {
					DPrintf("[Raft %d] sendHeartBeat: S%d has higher term %d > %d, step down", rf.me, server, reply.Term, rf.currentTerm)
					rf.becomeFollower(reply.Term)
				}
			}
		}(i, args[i])
	}
}

func (rf *Raft) sendNewCommand() {
	rf.mu.Lock()
	// DPrintf("[Raft %d] sendNewCommand: term=%d logLen=%d commitIdx=%d", rf.me, rf.currentTerm, len(rf.log), rf.commitIdx)

	args := make([]*AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		arg := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   rf.nextIdx[i] - 1,
			LeaderCommit: rf.commitIdx,
		}
		if arg.PrevLogIdx < 0 {
			arg.PrevLogTerm = -1
		} else {
			arg.PrevLogTerm = rf.log[arg.PrevLogIdx].Term
		}
		if len(rf.log)-1 > arg.PrevLogIdx {
			entries := rf.log[rf.nextIdx[i]:len(rf.log)]
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
			DPrintf("[Raft %d] sendNewCommand -> S%d: PrevLogIdx=%d Entries=%d", rf.me, server, arg.PrevLogIdx, len(arg.Entries))

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, arg, &reply, true)
			sendEndIdx := arg.PrevLogIdx + len(arg.Entries)
			if !reply.Success {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					DPrintf("[Raft %d] sendNewCommand: S%d has higher term %d > %d, step down", rf.me, server, reply.Term, rf.currentTerm)
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				} else { // log inconsistency, keep retry until success
					DPrintf("[Raft %d] sendNewCommand: S%d log inconsistency, retrying with lower PrevLogIdx", rf.me, server)
					rf.mu.Unlock()
					for !reply.Success {
						reply = AppendEntriesReply{}

						rf.mu.Lock()
						if reply.ConflictTerm != -1 {
							DPrintf("[Raft %d] Quick decrease nextIdx[%d] from %d to %d", rf.me, server, rf.nextIdx[server], reply.ConflictTermLogIdx)
							rf.nextIdx[server] = reply.ConflictTermLogIdx
						} else if rf.nextIdx[server] > 0 {
							DPrintf("[Raft %d] Slow decrease nextIdx[%d] from %d to %d", rf.me, server, rf.nextIdx[server], rf.nextIdx[server]-1)
							rf.nextIdx[server]--
						}

						if rf.role.Load() != Leader {
							rf.mu.Unlock()
							return
						}
						arg.PrevLogIdx = rf.nextIdx[server] - 1
						arg.PrevLogTerm = -1
						if arg.PrevLogIdx >= 0 {
							arg.PrevLogTerm = rf.log[arg.PrevLogIdx].Term
						}
						if len(rf.log)-1 > arg.PrevLogIdx {
							entries := rf.log[rf.nextIdx[i]:len(rf.log)]
							arg.Entries = make([]LogEntry, len(entries))
							copy(arg.Entries, entries)
						}
						rf.mu.Unlock()

						DPrintf("[Raft %d] sendNewCommand -> S%d retry: PrevLogIdx=%d Entries=%d", rf.me, server, arg.PrevLogIdx, len(arg.Entries))
						rf.sendAppendEntries(server, arg, &reply, true)
						sendEndIdx = arg.PrevLogIdx + len(arg.Entries)

						rf.mu.Lock()
						if rf.currentTerm < reply.Term {
							// DPrintf("[Raft %d] sendNewCommand: S%d has higher term %d > %d, step down", rf.me, server, reply.Term, rf.currentTerm)
							rf.becomeFollower(reply.Term)
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
			DPrintf("[Raft %d] sendNewCommand: S%d success, nextIdx=%d matchIdx=%d", rf.me, server, rf.nextIdx[server], rf.matchIdx[server])
			rf.triggerCommit()
			rf.mu.Unlock()
		}(i, args[i])
	}
}

func (rf *Raft) triggerCommit() {
	rf.mu.AssertHeld()

	for commitIdx := len(rf.log) - 1; commitIdx > rf.commitIdx; commitIdx-- {
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
			if rf.log[commitIdx].Term == rf.currentTerm {
				rf.commitEntries(commitIdx)
			}
			break
		}
	}
}

func (rf *Raft) commitEntries(newCommitIdx int) {
	DPrintf("[Raft %d] commitEntries: committing from %d to %d (newCommitIdx=%d, logLen=%d)",
		rf.me, rf.commitIdx+1, newCommitIdx, newCommitIdx, len(rf.log))
	for i := rf.commitIdx + 1; i <= newCommitIdx; i++ {
		// DPrintf("[Raft %d] commitEntries: applying index=%d cmd=%v term=%d",
		// 	rf.me, i, rf.log[i].Cmd, rf.log[i].Term)
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Cmd,
			CommandIndex: i + 1,
		}
	}
	rf.commitIdx = newCommitIdx
	// DPrintf("[Raft %d] commitEntries: done, commitIdx=%d", rf.me, rf.commitIdx)
}

func (rf *Raft) startPreVote() bool {
	rf.mu.Lock()
	DPrintf("[Raft %d] startPreVote: term=%d logLen=%d lastLogTerm=%d",
		rf.me, rf.currentTerm, len(rf.log), func() int {
			if len(rf.log) > 0 {
				return rf.log[len(rf.log)-1].Term
			}
			return -1
		}())

	args := make([]*PreVoteArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		arg := PreVoteArgs{
			LastLogIdx:  len(rf.log) - 1,
			LastLogTerm: -1,
		}
		if len(rf.log) > 0 {
			arg.LastLogTerm = rf.log[len(rf.log)-1].Term
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
				DPrintf("[Raft %d] startPreVote <- S%d: granted", rf.me, server)
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
				DPrintf("[Raft %d] startPreVote <- S%d: rejected (ok=%v grant=%v)", rf.me, server, ok, reply.VoteGranted)
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
		DPrintf("[Raft %d] startPreVote: won (approveCnt=%d/%d)", rf.me, preVoteApproveCnt.Load(), len(rf.peers))
		return true
	case <-loseC:
		DPrintf("[Raft %d] startPreVote: lost (rejectCnt=%d/%d)", rf.me, preVoteRejectCnt.Load(), len(rf.peers))
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
	rf.raiseTerm(rf.currentTerm + 1)
	rf.voteFor = rf.me
	rf.voteCnt = 1

	DPrintf("[Raft %d] startElection: term=%d logLen=%d lastLogTerm=%d", rf.me, rf.currentTerm, len(rf.log), func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Term
		}
		return -1
	}())

	args := make([]*RequestVoteArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		arg := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			LastLogIdx:  len(rf.log) - 1,
			LastLogTerm: -1,
		}
		if len(rf.log) > 0 {
			arg.LastLogTerm = rf.log[len(rf.log)-1].Term
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
				DPrintf("[Raft %d] vote from S%d is %v", rf.me, server, reply.VoteGranted)
				rf.mu.Lock()
				if reply.VoteGranted {
					rf.voteCnt++
					DPrintf("[Raft %d] got vote from S%d, voteCnt=%d/%d", rf.me, server, rf.voteCnt, len(rf.peers))
					if rf.voteCnt > len(rf.peers)/2 {
						firstWin.Do(func() {
							DPrintf("[Raft %d] won election with %d votes", rf.me, rf.voteCnt)
							select {
							case winC <- struct{}{}:
							default:
							}
						})
					}
				} else if reply.Term > rf.currentTerm { // stop election
					DPrintf("[Raft %d] S%d replied with higher term %d > %d, stop election", rf.me, server, reply.Term, rf.currentTerm)
					rf.becomeFollower(reply.Term)
					rf.electionCancelFunc()
				} else {
					DPrintf("[Raft %d] vote denied from S%d (reply.Term=%d)", rf.me, server, reply.Term)
				}
				rf.mu.Unlock()
			case <-ctx.Done():
				return
			}

		}(i, args[i], ctx)
	}
	select {
	case <-winC:
		DPrintf("[Raft %d] election won, becoming leader", rf.me)
		rf.mu.Lock()
		rf.becomeLeader()
		rf.mu.Unlock()
		rf.sendNewCommand()
	case <-rf.electionTimer.C:
		DPrintf("[Raft %d] election timed out, restarting", rf.me)
		rf.electionCancelFunc()
		rf.startElection()
	case term := <-rf.electionResetCh:
		DPrintf("[Raft %d] election reset by higher term %d, becoming follower", rf.me, term)
		rf.electionCancelFunc()
		rf.becomeFollower(term)
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

		currentTerm: 1,
		voteFor:     -1,
		log:         make([]LogEntry, 0),
		commitIdx:   -1,

		applyCh: applyCh,
	}
	rf.heartbeatTicker = time.NewTicker(HeartBeatInterval)
	rf.electionTimer = time.NewTimer(randomTime(ElectionTimeout, 2*ElectionTimeout))
	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))
	rf.mu.Lock()
	if me == 0 {
		rf.becomeLeader()
	} else {
		rf.becomeFollower(1)
	}
	// DPrintf("[Raft %d] Make: created peer, initialTerm=%d role=%v", rf.me, rf.currentTerm, rf.role.Load().(Role))
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.run()

	return rf
}

func (rf *Raft) raiseTerm(term int) {
	rf.mu.AssertHeld() // must hold lock
	DPrintf("[Raft %d] raiseTerm: %d -> %d", rf.me, rf.currentTerm, term)
	rf.currentTerm = term
	rf.voteFor = -1
}

func (rf *Raft) becomeFollower(term int) {
	rf.mu.AssertHeld() // must hold lock
	DPrintf("[Raft %d] becomeFollower: curTerm=%d newTerm=%d", rf.me, rf.currentTerm, term)
	if rf.currentTerm < term {
		rf.raiseTerm(term)
	}

	rf.role.Store(Follower)
	rf.resetElectionTimer() // start election timer
}

func (rf *Raft) becomeLeader() {
	rf.mu.AssertHeld() // must hold lock
	DPrintf("[Raft %d] becomeLeader: term=%d", rf.me, rf.currentTerm)
	rf.role.Store(Leader)

	for i := range rf.peers {
		rf.nextIdx[i] = len(rf.log)
		rf.matchIdx[i] = 0
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
	// DPrintf("[Raft %d] resetElectionTimer: %v", rf.me, d)
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
