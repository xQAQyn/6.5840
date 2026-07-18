package raft

// AppendEntries RPC: log replication and heartbeat mechanism.
// The leader sends AppendEntries to followers to replicate log entries
// and to maintain authority (heartbeat).

import (
	"time"
)

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
		reply.ConflictTerm = -1
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
			reply.ConflictTermLogIdx = len(rf.Log)
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

	// Append new entries per Raft Figure 2 rule 3: only delete existing
	// entries that CONFLICT with a new one (same index but different term).
	// An empty heartbeat, or a batch that is a prefix of what we already
	// have, must NOT truncate our tail -- otherwise a committed/replicated
	// entry can be destroyed and later overwritten (Figure 8 violation).
	insertAt := args.PrevLogIdx + 1
	for i, entry := range args.Entries {
		idx := insertAt + i
		if idx >= len(rf.Log) {
			// beyond our current log: append everything that remains
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
		if rf.Log[idx].Term == entry.Term {
			// same (index, term): already have this entry, keep it
			continue
		}
		// conflict at idx: never discard a committed entry
		if idx <= rf.commitIdx {
			Logf(rf.me, "append-entries-conflict-below-commit | from=S%d idx=%d commitIdx=%d entryTerm=%d myTerm=%d (refusing to truncate)",
				args.LeaderId, idx, rf.commitIdx, entry.Term, rf.Log[idx].Term)
			break
		}
		Logf(rf.me, "append-entries-truncate | from=S%d keeping[0..%d] overwriting[%d..%d]=%s",
			args.LeaderId, idx-1, idx, len(rf.Log)-1,
			fmtEntries(rf.Log[idx:], idx))
		rf.Log = rf.Log[:idx]
		rf.Log = append(rf.Log, args.Entries[i:]...)
		break
	}
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
