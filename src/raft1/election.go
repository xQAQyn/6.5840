package raft

// Election logic: when the election timer fires, a follower becomes a candidate,
// runs a PreVote phase, and if successful, requests votes from peers. The first
// candidate to receive a majority of votes becomes the leader.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

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
				if rf.role.Load() != Candidate {
					return
				}
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
