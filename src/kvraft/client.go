package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu       sync.Mutex
	leaderId int
	opId     int64
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.leaderId = 0
	ck.clientId = nrand()
	ck.opId = 0
	ck.mu = sync.Mutex{}

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		ClientId: ck.clientId,
		Key:      key,
		OpId:     atomic.AddInt64(&ck.opId, 1),
	}

	for {
		var reply GetReply

		doneChan := make(chan bool, 1)
		go func(leaderId int) {
			ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
			// DPrintf("Clerk[%d] tries to  get %v's value from [%d] \n", ck.clientId, key, leaderId)
			doneChan <- ok
		}(ck.leaderId)

		select {
		case <-time.After(500 * time.Millisecond): // retry after timeout
			DPrintf("Clerk[%d]'s PutAppendRPC  time out \n ", ck.clientId)
			continue
		case ok := <-doneChan:
			if !ok || reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			} else if reply.Err == ErrNoKey || reply.Err == ErrOutdate {
				return ""
			} else if reply.Err == OK {
				return reply.Value
			}
		}

	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		OpId:     atomic.AddInt64(&ck.opId, 1),
		ClientId: ck.clientId,
	}

	for {
		var reply PutAppendReply

		doneChan := make(chan bool, 1)
		go func(leaderId int) {
			ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
			doneChan <- ok
		}(ck.leaderId)

		select {
		case <-time.After(500 * time.Millisecond):
			DPrintf("Clerk[%d]'s PutAppendRPC  time out \n ", ck.clientId)
			continue
		case ok := <-doneChan:
			// DPrintf(" ok %v %v\n", ok, reply.Err)
			if !ok || reply.Err == ErrWrongLeader {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			} else {
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
