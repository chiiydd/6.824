package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] receives request from [%d] at term %d\n", rf.me, args.CandidateId, rf.currentTerm)

	term := args.Term

	if term > rf.currentTerm {
		rf.setNewTerm(term)
	}

	latestLogIndex := rf.log.latestIndex()
	latestLogTerm := max(rf.snapshotTerm, rf.log.at(latestLogIndex).Term)
	upToDate := latestLogTerm < args.LastLogTerm || (latestLogTerm == args.LastLogTerm && latestLogIndex <= args.LastLogIndex)

	DPrintf("[%d]'latestLog index is %d term is %d,uptoDate is %v,votedFor is %d\n", rf.me, latestLogIndex, latestLogTerm, upToDate, rf.votedFor)
	if term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("[%d] term %d rejects to vote for [%d] term %d\n", rf.me, rf.currentTerm, args.CandidateId, term)
	} else if (rf.votedFor == -1 || args.CandidateId == rf.votedFor) && upToDate {
		rf.votedFor = args.CandidateId
		rf.state = follower
		rf.resetElectionTimer()
		rf.persist()
		reply.VoteGranted = true
		DPrintf("[%d] vote for %d at term %v\n", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		reply.VoteGranted = false
		DPrintf("[%d] rejects to vote for [%d] at term %d\n", rf.me, args.CandidateId, rf.currentTerm)
		DPrintf("[%d] 's latestIndex is %d,but the candidate latestIndex %d\n", rf.me, rf.log.latestIndex(), args.LastLogIndex)
		// DPrintf("log:%v\n", rf.log)
	}
	reply.Term = rf.currentTerm

}

func (rf *Raft) setNewTerm(term int) {
	DPrintf("[%d]set new term %v (%d ->follower)\n", rf.me, term, rf.state)
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
	rf.persist()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

// func (rf *Raft) startElection(peerSum, term, candidateId, latestLogIndex, latestLogTerm int) bool {
// 	DPrintf("[%d] start an election at term %v\n", candidateId, term)
// 	voteCount := 1
// 	var countMu sync.Mutex

// 	cond := sync.NewCond(&countMu)
// 	fished := 1
// 	for i, _ := range rf.peers {
// 		if i == candidateId {
// 			continue
// 		}

// 		go func(peer int) {
// 			args := &RequestVoteArgs{
// 				Term:         term,
// 				CandidateId:  candidateId,
// 				LastLogIndex: latestLogIndex,
// 				LastLogTerm:  latestLogTerm,
// 			}

// 			reply := &RequestVoteReply{}
// 			// DPrintf("[%d] send requestVote to [%d] at term %d\n", candidateId, peer, term)
// 			ok := rf.sendRequestVote(peer, args, reply)
// 			if !ok {
// 				DPrintf("[%d]'RequestRPC sent to [%d] fails \n", candidateId, peer)
// 			}
// 			countMu.Lock()
// 			rf.mu.Lock()
// 			if rf.currentTerm == args.Term && reply.VoteGranted {
// 				voteCount++
// 				DPrintf("[%d] receives vote from [%d]\n", candidateId, peer)
// 			} else {
// 				if ok {
// 					DPrintf("[%d] receives rejection from [%d]\n", candidateId, peer)
// 				}

// 			}

// 			fished++
// 			rf.mu.Unlock()
// 			countMu.Unlock()
// 			cond.Broadcast()
// 		}(i)

// 	}

// 	countMu.Lock()
// 	defer countMu.Unlock()
// 	for voteCount <= peerSum/2 && fished < peerSum {
// 		cond.Wait()
// 	}

// 	return voteCount > peerSum/2

// }

// func (rf *Raft) checkElectionTimeout() {
// 	rf.mu.Lock()
// 	// timeout := time.Now().After(rf.lastHeartsBeat.Add(rf.electionTimeout))
// 	if time.Now().After(rf.electionTime) && rf.state != leader {
// 		rf.currentTerm++
// 		rf.state = candidate
// 		rf.votedFor = rf.me
// 		rf.resetElectionTimer()
// 		latestLogIndex := rf.log.latestIndex()
// 		latestLogTerm := max(rf.snapshotTerm, rf.log.at(latestLogIndex).Term)

// 		rf.persist()
// 		rf.mu.Unlock()

// 		becomeLeader := rf.startElection(len(rf.peers), term, rf.me, latestLogIndex, latestLogTerm)

// 		rf.mu.Lock()

// 		if becomeLeader && rf.currentTerm == term {
// 			DPrintf("[%d] become leader at term %d\n", rf.me, term)
// 			rf.state = leader
// 			next := max(rf.log.latestIndex(), 1)
// 			for i := 0; i < len(rf.peers); i++ {
// 				rf.nextIndex[i] = next
// 				// rf.matchIndex[i] = 0
// 			}
// 			rf.sendAppendsL(false)
// 		} else {
// 			DPrintf("[%d] fails in election at term %d \n", rf.me, term)
// 		}

// 	}

// 	rf.mu.Unlock()

// }

func (rf *Raft) checkElectionTimeout() {
	// timeout := time.Now().After(rf.lastHeartsBeat.Add(rf.electionTimeout))
	if time.Now().After(rf.electionTime) && rf.state != leader {
		DPrintf("[%d] at term %d change\n", rf.me, rf.currentTerm)
		rf.currentTerm++
		rf.state = candidate
		rf.votedFor = rf.me
		rf.resetElectionTimer()
		latestLogIndex := rf.log.latestIndex()
		latestLogTerm := max(rf.snapshotTerm, rf.log.at(latestLogIndex).Term)

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogTerm:  latestLogTerm,
			LastLogIndex: latestLogIndex,
		}
		vote := 1

		rf.persist()
		DPrintf("[%d]start election at term %d.\n", rf.me, rf.currentTerm)

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.requestVote(&args, i, &vote)
		}
	}
}

func (rf *Raft) requestVote(args *RequestVoteArgs, peer int, vote *int) {

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.VoteGranted && args.Term == rf.currentTerm && rf.state == candidate {
			*vote++
			DPrintf("[%d] receives [%d]'s vote at term %d", rf.me, peer, rf.currentTerm)
			if *vote == len(rf.peers)/2+1 {
				rf.state = leader
				DPrintf("[%d] become leader at term %d\n", rf.me, rf.currentTerm)
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.log.latestIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.sendAppendsL(false)
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.setNewTerm(reply.Term)
			}
		}

	} else {
		DPrintf("[%d] sending requestVoteRPC to [%d] fails at term %d\n", rf.me, peer, args.Term)
	}

}
