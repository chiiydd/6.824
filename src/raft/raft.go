package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type State int

const (
	follower State = iota
	candidate
	leader
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	log          Log
	votedFor     int
	currentTerm  int
	state        State
	electionTime time.Time

	// volatile state  on all servers 注释
	commitIndex int
	lastApplied int

	// volatile state on leaders

	nextIndex  []int
	matchIndex []int

	applyCh  chan ApplyMsg
	applyCon *sync.Cond

	// snapshot

	snapshotIndex int
	snapshotTerm  int

	applyMu sync.Mutex

	// waitingSnapshot []byte
	// waitingIndex    int
	// waitingTerm     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		DPrintf("[%d] reading from persist fails.\n", rf.me)

	} else {
		DPrintf("[%d] up from persist  voted for [%d] at term %d.\n", rf.me, votedFor, rf.currentTerm)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	return w.Bytes()
}

//
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// Done offset no use
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()

	DPrintf("[%d] Follower 's snapshotIndex %d latestIndext %d\n", rf.me, rf.snapshotIndex, rf.log.latestIndex())

	//InstallSnapshot rule 1
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// InstallSnapshot rule 5
	if args.LastIncludedIndex < rf.snapshotIndex || args.LastIncludedIndex < rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	DPrintf("[%d] receives [%d]'s snapshot(%d,%d)\n", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)

	// InstallSnapshot rule 6
	if rf.log.latestIndex() >= args.LastIncludedIndex {
		// DPrintf("[%d] log is %v\n", rf.me, rf.log)
		rf.log = makeLog(rf.log.slice(args.LastIncludedIndex+1, rf.log.latestIndex()), args.LastIncludedIndex)
	} else if rf.log.latestIndex() < args.LastIncludedIndex {

		rf.log = makeEmptyLog()
		rf.log.Index0 = args.LastIncludedIndex

	}
	//update  commitIndex lastApplied info
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.mu.Unlock()

	rf.applyMu.Lock()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg
	rf.applyMu.Unlock()

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	DPrintf("[%d] install snapshot \n", rf.me)
	defer rf.mu.Unlock()

	//  the snapshot is older than the server    Or    the index of snapshot hasn't commit
	if rf.snapshotIndex > index || index > rf.commitIndex {
		return
	}

	//update snapshot info
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log.at(index).Term
	rf.log = makeLog(rf.log.slice(index+1, rf.log.latestIndex()), index)

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)

}
func (rf *Raft) leaderSendSnapshot(peer int, args *InstallSnapshotArgs) {

	reply := &InstallSnapshotReply{}

	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		DPrintf("[%d] sending snapshot to [%d] fails.\n", rf.me, peer)
		return
	}
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > args.Term { // sending snapshot fails
			if reply.Term > rf.currentTerm {
				rf.setNewTerm(reply.Term)
			}
			DPrintf("[%d] sending snapshot to [%d] fails.\n", rf.me, peer)
			return
		}
		DPrintf("[%d] sending snapshot to [%d] succeeds.\n", rf.me, peer)
		//sending snapshot succeeds,update nextIndex and commit
		rf.nextIndex[peer] = max(rf.snapshotIndex+1, rf.nextIndex[peer])
		DPrintf("[%d]'s snapshotIndex is %d,[%d]'s nextIndex is %d &&&&\n", rf.me, rf.snapshotIndex, peer, rf.nextIndex[peer])
		rf.matchIndex[peer] = max(rf.snapshotIndex, rf.matchIndex[peer])
		// rf.commitRule()
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	// rule 2  for all servers
	if args.Term > rf.currentTerm {
		DPrintf("[%d] Follower is outdate at term %d.\n", rf.me, rf.currentTerm)
		rf.setNewTerm(args.Term)
		reply.Term = args.Term
		rf.resetElectionTimer()
		return
	}
	// AppendEntries rule 1
	if args.Term < rf.currentTerm {
		DPrintf("[%d] term %d,but [%d] leader is %d  (outdate)\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}
	rf.resetElectionTimer()

	if rf.state == candidate { //candidate rule 3
		rf.state = follower
		rf.votedFor = -1
	}

	// AppendEntries rule 2

	if args.PrevLogIndex > rf.log.latestIndex() {
		reply.ConflictIndex = rf.log.latestIndex()
		return
	}
	if args.PrevLogIndex < rf.snapshotIndex {

		DPrintf("[%d]'s entries outdate\n", args.LeaderId)
		return
	}
	if rf.snapshotIndex < args.PrevLogIndex && rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		conflictTerm := rf.log.at(args.PrevLogIndex).Term
		for i := args.PrevLogIndex; i > rf.snapshotIndex; i-- { //fix bug  for 2D   >=0  ->    >rf.snapshotIndex
			if rf.log.at(i).Term != conflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		return
	}
	if rf.snapshotIndex == args.PrevLogIndex && rf.snapshotTerm != args.PrevLogTerm {
		DPrintf("[%d] snapshotIndex %d,snapshotTerm %d  %d\n", rf.me, rf.snapshotIndex, rf.snapshotTerm, args.PrevLogTerm)
		return
	}
	latestLogIndex := rf.log.latestIndex()
	// latestLogTerm := max(rf.snapshotTerm, rf.log.at(latestLogIndex).Term)
	for idx, entry := range args.Entries {

		//  AppendEntries rule 3
		if entry.Index <= latestLogIndex && entry.Term != rf.log.at(entry.Index).Term {
			rf.log.cutEnd(entry.Index)
			latestLogIndex = rf.log.latestIndex()
			// latestLogTerm = max(rf.snapshotTerm, rf.log.at(latestLogIndex).Term)
		}
		//  AppendEntries rule 4
		if entry.Index > latestLogIndex {
			rf.log.Log = append(rf.log.Log, args.Entries[idx:]...)
			DPrintf("[%d] succeed receive leader [%d]'s entries,now latest log index %d\n", rf.me, args.LeaderId, rf.log.latestEntry().Index)
			// for i := idx; i < len(args.Entries); i++ {
			// 	DPrintf("(%d %d) ", args.Entries[i].Index, args.Entries[i].Term)
			// }
			// DPrintf("\n")
			break
		}
	}

	// rf.log.Log = append(rf.log.Log[:args.PrevLogIndex+1-rf.log.Index0], args.Entries...)

	if args.LeaderCommit > rf.commitIndex {

		rf.commitIndex = min(args.LeaderCommit, rf.log.latestIndex())
		// rf.apply()
	}
	rf.apply()
	reply.Success = true
	DPrintf("[%d] succeed receive leader [%d]'s entries at term %d\n", rf.me, args.LeaderId, rf.currentTerm)
	rf.resetElectionTimer()
	DPrintf("[%d]'s commitIndex is %d,and leader[%d]'s is %d\n", rf.me, rf.commitIndex, args.LeaderId, args.LeaderCommit)
	// DPrintf("[%d] entries update to %v\n", rf.me, rf.log)

}

func (rf *Raft) appendEntriesToPeer(heartbeat bool, peer int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.state == leader && rf.log.latestIndex() >= rf.nextIndex[peer]) || (rf.state == leader && heartbeat) {

		if rf.nextIndex[peer] <= rf.snapshotIndex {

			DPrintf("FOLLOWER[%d]'nextIndex is %d\n", peer, rf.nextIndex[peer])
			DPrintf("[%d] snapshotIndex %d latestlogIndex %d\n", rf.me, rf.snapshotIndex, rf.log.latestIndex())
			args := &InstallSnapshotArgs{

				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm:  rf.snapshotTerm,
				Data:              rf.persister.ReadSnapshot(),
			}

			rf.mu.Unlock()
			rf.leaderSendSnapshot(peer, args)
			rf.mu.Lock() // lock  before  defer unlock
			return
		}
		prevIndex := rf.nextIndex[peer] - 1
		preTerm := rf.log.at(prevIndex).Term

		if preTerm == 0 {
			preTerm = rf.snapshotTerm
		}
		latestIndex := rf.log.latestIndex()
		args := &AppendEntriesArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  preTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      make([]entry, rf.log.latestIndex()-prevIndex),
		}

		// DPrintf("[%d]'s log %v\n", rf.me, rf.log)
		copy(args.Entries, rf.log.slice(prevIndex+1, latestIndex))
		// DPrintf("[%d] appends entries %v to[%d]\n", rf.me, args.Entries, peer)
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()
		ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		if !ok {
			DPrintf("[%d]'s AppendEntriesRPC to [%d] fails at term %d\n", rf.me, peer, args.Term)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term)
		} else if rf.currentTerm == args.Term && rf.state == leader {
			if reply.Success {
				match := args.PrevLogIndex + len(args.Entries)
				next := match + 1
				DPrintf("[%d] appends entries to [%d] succeed ,matchIndex  %d\n", rf.me, peer, match)
				rf.matchIndex[peer] = max(rf.matchIndex[peer], match)
				rf.nextIndex[peer] = max(rf.nextIndex[peer], next)
				// DPrintf("[%d] AppendEntries to [%d] success: next %d,match %d at term %d\n ", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.currentTerm)
				rf.commitRule()
			} else if reply.ConflictIndex != -1 {
				rf.nextIndex[peer] = max(1, reply.ConflictIndex)
			} else {

				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
					DPrintf("")
				}

			}
		}

	}

}
func (rf *Raft) commitRule() {
	if rf.state != leader {
		return
	}

	numServer := len(rf.peers)
	for i := rf.commitIndex + 1; i <= rf.log.latestIndex(); i++ {
		if rf.log.at(i).Term != rf.currentTerm {
			continue
		}
		count := 1
		for serverId := 0; serverId < numServer; serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= i {
				count++
			}
			if count > numServer/2 {
				DPrintf("[%d]'s trying to commit  index %d %d \n", rf.me, i, rf.log.at(i).Term)
				DPrintf("%v\n", rf.matchIndex)
				rf.commitIndex = i
				//TODO: apply
				// rf.apply()
				// rf.persist()
				break
			}
		}

	}
	DPrintf("[%d] %v\n", rf.me, rf.matchIndex)
	rf.apply()
}

func (rf *Raft) sendAppendsL(heartbeat bool) {

	// rf.mu.Lock()
	// if rf.state != leader {
	// 	rf.mu.Unlock()
	// 	return
	// }
	// rf.resetElectionTimer()
	// rf.mu.Unlock()
	for peer, _ := range rf.peers {

		if peer != rf.me {
			go rf.appendEntriesToPeer(heartbeat, peer)
		}

	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.log.latestIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == leader
	if isLeader == false {
		return -1, term, false
	}

	rf.log.append(entry{Index: index, Command: command, Term: term})
	rf.persist()
	DPrintf("[%d]'s append a new entry %d at term %d\n", rf.me, index, rf.currentTerm)
	rf.sendAppendsL(false)
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("[%d] is now down.\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		if rf.state == leader {
			rf.sendAppendsL(true)
			rf.resetElectionTimer()
		}
		rf.checkElectionTimeout()
		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond)

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.state = follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = makeEmptyLog()

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// snapshot initial
	rf.snapshotIndex = -1
	rf.snapshotTerm = 0
	// for i := 0; i < len(peers); i++ {
	// 	rf.nextIndex[i] = 1
	// }
	rf.applyCh = applyCh
	rf.applyCon = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("[%d] is now up\n", rf.me)
	if rf.snapshotIndex > 0 {
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
		rf.log = makeLog(rf.log.slice(rf.snapshotIndex+1, rf.log.latestIndex()), rf.snapshotIndex)

		// msg := ApplyMsg{
		// 	SnapshotValid: true,
		// 	Snapshot:      rf.persister.ReadSnapshot(),
		// 	SnapshotIndex: rf.snapshotIndex,
		// 	SnapshotTerm:  rf.snapshotTerm,
		// }
		// rf.applyCh <- msg
		go rf.deliverSnapshotMsg()
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.latestIndex() + 1
	}
	rf.resetElectionTimer()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
func (rf *Raft) deliverSnapshotMsg() {

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotIndex: rf.snapshotIndex,
		SnapshotTerm:  rf.snapshotTerm,
	}
	rf.applyCh <- msg
}

func (rf *Raft) apply() {

	rf.applyCon.Broadcast()

}

func (rf *Raft) applier() {
	rf.mu.Lock()

	defer rf.mu.Unlock()

	for !rf.killed() {

		// if rf.lastApplied+1 <= rf.commitIndex && rf.lastApplied+1 <= rf.log.latestIndex() && rf.lastApplied+1 > rf.log.start() {
		// 	rf.lastApplied++
		// 	DPrintf("[%d]'Applier deliver %v (index0 is %d)\n", rf.me, rf.lastApplied, rf.log.Index0)
		// 	am := ApplyMsg{
		// 		CommandValid: true,
		// 		Command:      rf.log.at(rf.lastApplied).Command,
		// 		CommandIndex: rf.lastApplied,
		// 	}
		// 	rf.mu.Unlock()
		// 	rf.applyCh <- am
		//  rf.mu.lock()
		// } else {
		// 	rf.applyCon.Wait()
		// }

		//  index0>0 means it has snapshot
		if rf.log.start() > 0 && rf.lastApplied+1 <= rf.log.start() {
			rf.applyCon.Wait()
		} else {

			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex && i <= rf.log.latestIndex(); i++ {
				rf.lastApplied = i

				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log.at(i).Command,
					CommandIndex: i,
					CommandTerm:  rf.log.at(i).Term,
				})
			}
			rf.mu.Unlock()
			rf.applyMu.Lock()
			for _, msg := range msgs {
				DPrintf("[%d]  apply %d %v\n", rf.me, msg.CommandIndex, msg.Command)
				rf.applyCh <- msg
			}
			rf.applyMu.Unlock()
			rf.mu.Lock()
			rf.applyCon.Wait()
		}

	}
}

func (rf *Raft) resetElectionTimer() {
	// DPrintf("[%d]reset ElectionTimer at term %d\n", rf.me, rf.currentTerm)
	rf.electionTime = time.Now().Add(getRandomTimeout())
}

func (rf *Raft) isMajorityLeader() bool {

	num := 1
	majority := false
	args := AppendEntriesArgs{}
	for i, _ := range rf.peers {
		if i != rf.me {

			go func(peer int) {
				var reply AppendEntriesReply
				ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
				if ok {
					rf.mu.Lock()
					num++
					if num == len(rf.peers)/2+1 && rf.state == leader {
						majority = true
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}

	time.Sleep(100 * time.Millisecond)
	return majority

}

func (rf *Raft) GetRaftStateSize() int {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}
