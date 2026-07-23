package raft

import "time"

// RequestVote RPC: candidates solicit votes from peers during elections.
// PreVote RPC: a preliminary check before incrementing term, to avoid
// disrupting the cluster when a partitioned node has a stale log.

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
	type result struct {
		ok    bool
		reply PreVoteReply
	}
	done := make(chan result, 1)
	go func() {
		rep := PreVoteReply{}
		ok := rf.peers[server].Call("Raft.PreRequestVote", args, &rep)
		done <- result{ok, rep}
	}()
	select {
	case r := <-done:
		*reply = r.reply
		return r.ok
	case <-time.After(PreVoteRPCTimeout):
		// Peer did not answer in time (most likely a partitioned link whose
		// failure reply labrpc would otherwise delay ~7s). Treat as a no-vote
		// so startPreVote can fail fast and the election retries on the timer.
		// The in-flight Call is abandoned; it finishes on its own and the
		// buffered channel prevents it from leaking.
		return false
	}
}
