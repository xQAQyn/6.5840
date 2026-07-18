package raft

// Log replication: the leader sends AppendEntries to followers to replicate
// new log entries, handles retries on inconsistency, and advances the commit
// index when a majority of followers have replicated an entry.

import (
	"6.5840/raftapi"
)

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
						if rf.role.Load() != Leader || arg.Term < rf.CurrentTerm {
							rf.mu.Unlock()
							return
						}
						// Conflict reply: the follower reports ConflictTermLogIdx, which is
						// either the first index of the conflicting term (ConflictTerm != -1)
						// or the length of the follower's log (ConflictTerm == -1, log too
						// short). Both cases resolve by jumping nextIdx to ConflictTermLogIdx;
						// never decrement by 1 per round-trip (that is O(gap) and starves
						// catch-up under leader churn / unreliable networks).
						Logf(rf.me, "replicate-retry | to=S%d conflict-jump nextIdx[%d]: %d -> %d (conflictTerm=%d)",
							server, server, rf.nextIdx[server], reply.ConflictTermLogIdx, reply.ConflictTerm)
						rf.nextIdx[server] = reply.ConflictTermLogIdx

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
