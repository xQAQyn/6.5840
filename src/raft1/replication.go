package raft

// Log replication: the leader sends AppendEntries to followers to replicate
// new log entries, handles retries on inconsistency, and advances the commit
// index when a majority of followers have replicated an entry. When a follower
// is so far behind that the needed entries have already been discarded into a
// snapshot, the leader sends an InstallSnapshot RPC instead.

// replResult guides the per-peer replication loop.
type replResult int

const (
	replDone replResult = iota // follower caught up (or transient failure); stop
	replAbort                  // we are no longer the leader / stepped down; stop
	replNeedSnap              // follower needs a snapshot; loop back and send one
)

// sendNewCommand replicates the leader's log to every follower. It is invoked on
// new commands, on leader election, and on each heartbeat tick.
func (rf *Raft) sendNewCommand() {
	rf.mu.Lock()
	if rf.role.Load() != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateToOne(i)
	}
}

// replicateToOne drives one follower toward the leader's log, retrying through
// log conflicts. If the follower needs entries that the leader has discarded
// into its snapshot, it first sends an InstallSnapshot, then continues with
// AppendEntries.
func (rf *Raft) replicateToOne(server int) {
	for {
		rf.mu.Lock()
		if rf.role.Load() != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.CurrentTerm
		needSnap := rf.nextIdx[server]-1 < rf.lastIncludedIndex
		var snapArgs *InstallSnapshotArgs
		if needSnap {
			snapArgs = &InstallSnapshotArgs{
				Term:               term,
				LeaderId:           rf.me,
				LastIncludedIndex:  rf.lastIncludedIndex,
				LastIncludedTerm:   rf.lastIncludedTerm,
				Data:               rf.snapshot,
			}
		}
		rf.mu.Unlock()

		if needSnap {
			reply := InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(server, snapArgs, &reply)
			rf.mu.Lock()
			if rf.role.Load() != Leader || rf.CurrentTerm != term {
				rf.mu.Unlock()
				return
			}
			if !ok {
				// network failure; retry on the next heartbeat tick.
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.CurrentTerm {
				Logf(rf.me, "step-down | from=S%d theirTerm=%d myTerm=%d reason=higher-term-install-snapshot-reply",
					server, reply.Term, rf.CurrentTerm)
				rf.becomeFollower(reply.Term, "higher-term-install-snapshot-reply")
				rf.mu.Unlock()
				return
			}
			// Snapshot applied: the follower now has everything up to the
			// snapshot boundary. Resume replication from there.
			rf.nextIdx[server] = rf.lastIncludedIndex + 1
			rf.matchIdx[server] = rf.lastIncludedIndex
			Logf(rf.me, "install-snapshot-ok | to=S%d nextIdx=%d matchIdx=%d",
				server, rf.nextIdx[server], rf.matchIdx[server])
			rf.mu.Unlock()
		}

		res := rf.replicateAppendEntries(server, term)
		switch res {
		case replNeedSnap:
			continue // follower fell further behind; send a snapshot then retry AE
		default:
			return
		}
	}
}

// buildAppendEntries constructs the AppendEntries args for `server`. The caller
// holds rf.mu, is the leader, and guarantees nextIdx[server]-1 >= lastIncludedIndex.
func (rf *Raft) buildAppendEntries(server int) *AppendEntriesArgs {
	arg := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIdx:   rf.nextIdx[server] - 1,
		LeaderCommit: rf.commitIdx,
	}
	// Safety: nextIdx may point past the end (e.g. after the log was trimmed);
	// clamp it to one-past-the-last.
	if arg.PrevLogIdx > rf.lastLogIndex() {
		rf.nextIdx[server] = rf.lastLogIndex() + 1
		arg.PrevLogIdx = rf.nextIdx[server] - 1
	}
	arg.PrevLogTerm = rf.getLogTerm(arg.PrevLogIdx)
	if rf.lastLogIndex() > arg.PrevLogIdx {
		entries := rf.Log[rf.toSliceIdx(rf.nextIdx[server]):]
		arg.Entries = make([]LogEntry, len(entries))
		copy(arg.Entries, entries)
	}
	return &arg
}

// replicateAppendEntries sends AppendEntries to one follower, retrying through
// log conflicts (using the follower's ConflictTerm hint for fast back-off)
// until the follower's log matches the leader's, then advances nextIdx/matchIdx
// and tries to advance commitIdx. term is the leader's term at dispatch; if it
// no longer matches we abort.
func (rf *Raft) replicateAppendEntries(server int, term int) replResult {
	rf.mu.Lock()
	if rf.role.Load() != Leader || rf.CurrentTerm != term {
		rf.mu.Unlock()
		return replAbort
	}
	// The follower needs entries inside our snapshot -> switch to InstallSnapshot.
	if rf.nextIdx[server]-1 < rf.lastIncludedIndex {
		rf.mu.Unlock()
		return replNeedSnap
	}
	arg := rf.buildAppendEntries(server)
	rf.mu.Unlock()

	if len(arg.Entries) > 0 {
		Logf(rf.me, "replicate | to=S%d prevIdx=%d nEntries=%d entries=%s",
			server, arg.PrevLogIdx, len(arg.Entries), fmtEntries(arg.Entries, arg.PrevLogIdx+1))
	} else {
		VLogf(rf.me, "heartbeat | to=S%d prevIdx=%d", server, arg.PrevLogIdx)
	}

	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, arg, &reply, true)
	sendEndIdx := arg.PrevLogIdx + len(arg.Entries)

	for !reply.Success {
		rf.mu.Lock()
		if rf.role.Load() != Leader || rf.CurrentTerm != term {
			rf.mu.Unlock()
			return replAbort
		}
		if rf.CurrentTerm < reply.Term {
			Logf(rf.me, "step-down | from=S%d theirTerm=%d myTerm=%d reason=higher-term-replicate-reply",
				server, reply.Term, rf.CurrentTerm)
			rf.becomeFollower(reply.Term, "higher-term-replicate-reply")
			rf.mu.Unlock()
			return replAbort
		}
		// Log inconsistency: jump nextIdx per the follower's conflict hint.
		// ConflictTermLogIdx is either the first index of the conflicting term
		// (ConflictTerm != -1) or the length of the follower's log as a logical
		// index (ConflictTerm == -1, log too short). Both resolve by jumping
		// nextIdx to ConflictTermLogIdx; never decrement by 1 per round-trip
		// (that is O(gap) and starves catch-up under churn / unreliable nets).
		Logf(rf.me, "replicate-retry | to=S%d prevIdx=%d conflictTerm=%d conflictIdx=%d",
			server, rf.nextIdx[server], reply.ConflictTerm, reply.ConflictTermLogIdx)
		rf.nextIdx[server] = reply.ConflictTermLogIdx

		// Conflict resolution pushed us into the discarded (snapshot) region:
		// the follower is too far behind for AppendEntries; send a snapshot.
		if rf.nextIdx[server]-1 < rf.lastIncludedIndex {
			rf.mu.Unlock()
			return replNeedSnap
		}

		arg.PrevLogIdx = rf.nextIdx[server] - 1
		arg.PrevLogTerm = rf.getLogTerm(arg.PrevLogIdx)
		if rf.lastLogIndex() > arg.PrevLogIdx {
			entries := rf.Log[rf.toSliceIdx(rf.nextIdx[server]):]
			arg.Entries = make([]LogEntry, len(entries))
			copy(arg.Entries, entries)
		} else {
			arg.Entries = nil
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
		if rf.role.Load() != Leader || rf.CurrentTerm != term {
			rf.mu.Unlock()
			return replAbort
		}
		if rf.CurrentTerm < reply.Term {
			Logf(rf.me, "step-down | from=S%d theirTerm=%d myTerm=%d reason=higher-term-retry-reply",
				server, reply.Term, rf.CurrentTerm)
			rf.becomeFollower(reply.Term, "higher-term-retry-reply")
			rf.mu.Unlock()
			return replAbort
		}
		rf.mu.Unlock()
	}

	// Success: update nextIdx/matchIdx and try to advance commitIdx.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role.Load() != Leader || rf.CurrentTerm != term {
		return replAbort
	}
	rf.nextIdx[server] = sendEndIdx + 1
	rf.matchIdx[server] = sendEndIdx
	Logf(rf.me, "replicate-ok | to=S%d nextIdx=%d matchIdx=%d",
		server, rf.nextIdx[server], rf.matchIdx[server])
	rf.triggerCommit()
	return replDone
}

// sendHeartBeat sends empty AppendEntries (heartbeats) to all followers. Not
// used by run() (which uses sendNewCommand for both heartbeats and replication),
// but kept for completeness and updated to logical indices.
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()

	VLogf(rf.me, "send-heartbeat | term=%d lastLogIdx=%d commitIdx=%d", rf.CurrentTerm, rf.lastLogIndex(), rf.commitIdx)

	args := make([]*AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		arg := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   rf.lastLogIndex(),
			PrevLogTerm:  rf.lastLogTerm(),
			LeaderCommit: rf.commitIdx,
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

// triggerCommit advances commitIdx to the highest index N such that a majority
// of peers have replicated the entry at N AND that entry is from the current
// term (the Figure 8 / Figure 2 no-op-commit rule). Finding one such index is
// sufficient: everything below it is committed transitively.
func (rf *Raft) triggerCommit() {
	rf.mu.AssertHeld()

	for idx := rf.lastLogIndex(); idx > rf.commitIdx; idx-- {
		cnt := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIdx[i] >= idx {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			if rf.getLogTerm(idx) == rf.CurrentTerm {
				Logf(rf.me, "trigger-commit | checkIdx=%d entryTerm=%d curTerm=%d votes=%d/%d matchIdx=%v",
					idx, rf.getLogTerm(idx), rf.CurrentTerm, cnt, len(rf.peers), rf.matchIdx)
				rf.advanceCommit(idx)
			} else {
				Logf(rf.me, "trigger-commit-skip | idx=%d entryTerm=%d curTerm=%d (will not commit from past term)",
					idx, rf.getLogTerm(idx), rf.CurrentTerm)
			}
			break
		}
	}
}

// advanceCommit moves commitIdx forward (never backward) and wakes the applier
// goroutine so it sends the newly-committed entries on applyCh.
func (rf *Raft) advanceCommit(newCommitIdx int) {
	rf.mu.AssertHeld()
	if newCommitIdx <= rf.commitIdx {
		return
	}
	Logf(rf.me, "commit | %d -> %d logLen=%d", rf.commitIdx, newCommitIdx, len(rf.Log))
	rf.commitIdx = newCommitIdx
	rf.applyCond.Signal()
}
