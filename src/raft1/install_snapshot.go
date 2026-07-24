package raft

// InstallSnapshot RPC: when a follower is so far behind that the leader has
// already discarded the log entries it needs, the leader sends its snapshot
// (plus the boundary index/term) so the follower can replace its state.
// Section 7 of the extended Raft paper. We send the whole snapshot in one RPC
// (no Figure 13 offset/chunking), as instructed.

import "time"

// InstallSnapshotArgs is sent by the leader. Data is the opaque service
// snapshot; Term/LastIncludedIndex/LastIncludedTerm describe the snapshot's
// boundary so the follower can splice it into its own log.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply. Success is informational; followers re-apply idempotently.
type InstallSnapshotReply struct {
	Term    int
	Success bool
}

// InstallSnapshot is the follower-side handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Logf(rf.me, "install-snapshot-recv | from=S%d term=%d lastIncludedIdx=%d lastIncludedTerm=%d dataLen=%d myLastIncludedIdx=%d myLastLogIdx=%d",
		args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data),
		rf.lastIncludedIndex, rf.lastLogIndex())

	if args.Term < rf.CurrentTerm {
		// Stale leader: refuse and report our term.
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// Valid leader: become/remain follower and reset the election timer.
	rf.becomeFollower(args.Term, "install-snapshot")

	// Ignore snapshots that are not newer than what we already have, so the
	// service's state never moves backwards.
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = true
		return
	}

	// Figure 12 rule 7/8: if we already have an entry at LastIncludedIndex with
	// a matching term, our log shares this prefix, so retain the tail beyond
	// it; otherwise discard the entire log and rely on the snapshot alone.
	oldLII := rf.lastIncludedIndex
	retain := false
	if args.LastIncludedIndex <= rf.lastLogIndex() && rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		retain = true
	}
	if retain {
		// Keep entries with logical index > args.LastIncludedIndex. Copy into a
		// fresh backing array so the discarded prefix is GC-able.
		keepFrom := args.LastIncludedIndex - oldLII // == toSliceIdx(LII+1) under oldLII
		trimmed := make([]LogEntry, len(rf.Log)-keepFrom)
		copy(trimmed, rf.Log[keepFrom:])
		rf.Log = trimmed
	} else {
		rf.Log = make([]LogEntry, 0)
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	// The snapshot is committed by construction; commitIdx never lags it.
	if rf.commitIdx < args.LastIncludedIndex {
		rf.commitIdx = args.LastIncludedIndex
	}
	// lastApplied is left for the applier goroutine to advance (it sends the
	// snapshot up applyCh). But never let it point below the new boundary.
	if rf.lastApplied > rf.lastIncludedIndex {
		// we had already applied past the snapshot; the service is ahead, so the
		// applier will skip sending it (lastApplied >= lastIncludedIndex).
	}

	rf.persist()

	Logf(rf.me, "install-snapshot-applied | lastIncludedIdx=%d lastIncludedTerm=%d keptEntries=%d commitIdx=%d",
		rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.Log), rf.commitIdx)

	reply.Term = rf.CurrentTerm
	reply.Success = true

	// Wake the applier so it delivers the snapshot to the service (outside the
	// lock) and then resumes applying any committed tail.
	rf.applyCond.Signal()
}

// sendInstallSnapshot sends one InstallSnapshot RPC, retrying transient network
// failures while we remain the leader. The RPC is idempotent, so re-sending is
// safe. Returns false if the call never succeeded.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	VLogf(rf.me, "send-install-snapshot | ->S%d term=%d lastIncludedIdx=%d dataLen=%d",
		server, args.Term, args.LastIncludedIndex, len(args.Data))
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for !ok && rf.role.Load() == Leader {
		time.Sleep(QuickRetryTime)
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	}
	VLogf(rf.me, "send-install-snapshot | <-S%d ok=%v replyTerm=%d success=%v",
		server, ok, reply.Term, reply.Success)
	return ok
}
