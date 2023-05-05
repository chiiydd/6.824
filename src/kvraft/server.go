package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key      string
	Value    string
	Command  string
	OpId     int64
	ClientId int64
}
type ApplyNotifyMsg struct {
	Err      Err
	Value    string
	ClientId int64
	Term     int
}

type Reply struct {
	OpId int64
	Msg  ApplyNotifyMsg
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db          map[string]string
	latestReply map[int64]Reply
	replyChan   map[int]chan ApplyNotifyMsg
	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	if latestReply, ok := kv.latestReply[args.ClientId]; ok && latestReply.OpId >= args.OpId {
		if latestReply.OpId == args.OpId {
			reply.Err = latestReply.Msg.Err
			reply.Value = latestReply.Msg.Value
		} else {
			reply.Value = ""
			reply.Err = ErrOutdate
		}

		kv.mu.Unlock()
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    "",
		Command:  "Get",
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := make(chan ApplyNotifyMsg, 1)

	kv.replyChan[index] = ch

	kv.mu.Unlock()

	select {
	case replyMsg := <-ch:
		if replyMsg.ClientId == args.ClientId && replyMsg.Term == term {
			reply.Value = replyMsg.Value
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}

	case <-time.After(750 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.replyChan, index)
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.x
	kv.mu.Lock()
	if latestReply, ok := kv.latestReply[args.ClientId]; ok && latestReply.OpId >= args.OpId {
		reply.Err = latestReply.Msg.Err
		if latestReply.OpId > args.OpId {
			reply.Err = ErrOutdate
		}
		kv.mu.Unlock()
		return
	}

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Command:  args.Op,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := make(chan ApplyNotifyMsg, 1)

	kv.replyChan[index] = ch
	kv.mu.Unlock()

	select {
	case replyMsg := <-ch:

		if replyMsg.ClientId == args.ClientId && replyMsg.Term == term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}

	case <-time.After(750 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.replyChan, index)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.

}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	// You may need initialization code here.
	kv.latestReply = make(map[int64]Reply)
	kv.replyChan = make(map[int]chan ApplyNotifyMsg)
	kv.db = make(map[string]string)
	kv.lastApplied = 0
	go kv.notifier()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	if kv.maxraftstate != -1 {
		go kv.snapshotChecker()
	}

	return kv
}

func (kv *KVServer) notifier() {

	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			// DPrintf("[%d] receives new applyMsg %v\n", kv.me, applyMsg)
			if applyMsg.CommandValid {
				kv.applyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				kv.applySnapshot(applyMsg)
			} else {
				DPrintf("[%d] receives wrong msg\n", kv.me)
			}
		}
	}
}

func (kv *KVServer) applyCommand(msg raft.ApplyMsg) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var notifyMsg ApplyNotifyMsg
	op := msg.Command.(Op)
	index := msg.CommandIndex
	notifyMsg.Term = msg.CommandTerm
	notifyMsg.ClientId = op.ClientId

	// check the operation whether has been applied.
	if reply, ok := kv.latestReply[op.ClientId]; ok && reply.OpId >= op.OpId {
		return
	}
	if index <= kv.lastApplied {
		return
	}

	kv.lastApplied = index

	if op.Command == "Get" {
		if value, ok := kv.db[op.Key]; ok {
			notifyMsg = ApplyNotifyMsg{
				Err:   OK,
				Value: value,
			}
		} else {
			notifyMsg = ApplyNotifyMsg{
				Err:   ErrNoKey,
				Value: "",
			}
		}
	} else if op.Command == "Put" {
		kv.db[op.Key] = op.Value
		notifyMsg = ApplyNotifyMsg{
			Err:   OK,
			Value: "",
		}
	} else if op.Command == "Append" {
		old, ok := kv.db[op.Key]
		if ok {
			kv.db[op.Key] = old + op.Value
		} else {
			kv.db[op.Key] = op.Value
		}

		notifyMsg = ApplyNotifyMsg{
			Err:   OK,
			Value: "",
		}
	}

	if ch, ok := kv.replyChan[index]; ok {
		ch <- notifyMsg

		DPrintf("[%d] succeeds in dealing with command %v key %v", kv.me, op.Command, op.Key)
		if op.Command == "Get" {
			DPrintf("%v\n", kv.db[op.Key])
		} else {
			DPrintf("%v\n", op.Value)
		}
		// DPrintf("%v \n", kv.latestReply[op.ClientId])

	}
	kv.latestReply[op.ClientId] = Reply{
		OpId: op.OpId,
		Msg:  notifyMsg,
	}

}

func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var db map[string]string
	var latestReply map[int64]Reply

	r := bytes.NewBuffer(msg.Snapshot)

	d := labgob.NewDecoder(r)
	if d.Decode(&db) != nil || d.Decode(&latestReply) != nil {
		fmt.Printf("[%d] decoding snapshot data fails \n", kv.me)
		return
	}

	// update db  when the snapshot is up-to-date
	if msg.SnapshotIndex >= kv.lastApplied {
		DPrintf("[%d] apply Snapshot   \n", kv.me)
		kv.lastApplied = msg.SnapshotIndex
		kv.latestReply = latestReply
		kv.db = db
	}
}

func (kv *KVServer) snapshotChecker() {
	var snapshot []byte
	var latestIncludedIndex int

	for !kv.killed() {
		if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			kv.mu.Lock()
			e.Encode(kv.db)
			e.Encode(kv.latestReply)
			snapshot = w.Bytes()
			latestIncludedIndex = kv.lastApplied
			kv.mu.Unlock()

			kv.rf.Snapshot(latestIncludedIndex, snapshot)
		}
		time.Sleep(40 * time.Millisecond)
	}

}
