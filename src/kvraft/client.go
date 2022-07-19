package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id         int64 // identifier of the client
	lastLeader int   // remember the last leader
	requestId  int   // identifier of the request from the client
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
	ck.id = nrand()
	ck.lastLeader = 0
	ck.requestId = 0
	time.Sleep(200 * time.Millisecond) // wait for Raft to reach alignment
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

	ck.requestId++

	args := &GetArgs{
		Key:       key,
		ClientId:  ck.id,
		RequestId: ck.requestId,
	}

	cur := ck.lastLeader
	DPrintf("Client %d, waiting %s, from %d", ck.id, key, cur)
	for {
		reply := &GetReply{
			Err:   "",
			Value: "",
		}
		ok := ck.servers[cur].Call("KVServer.Get", args, reply)
		if ok {
			DPrintf("Client %d, done %s, from %d, %s", ck.id, key, cur, reply.Err)
			if reply.Err == OK {
				ck.lastLeader = cur
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.lastLeader = cur
				return ""
			}
		}
		DPrintf("Client %d, Retrying %s, Fail from %d, %s", ck.id, key, cur, reply.Err)
		cur++
		cur = cur % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}

	DPrintf("Clerk finished one Get call")
	return ""
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

	ck.requestId++

	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.id,
		RequestId: ck.requestId,
	}

	cur := ck.lastLeader
	for {
		DPrintf("Client %d, waiting %s, %s, %s, from %d", ck.id, op, key, value, cur)
		reply := &PutAppendReply{
			Err: "",
		}
		ok := ck.servers[cur].Call("KVServer.PutAppend", args, reply)
		if ok {
			DPrintf("Client %d, done %s, %s, %s, from %d, %s", ck.id, op, key, value, cur, reply.Err)
			if reply.Err == OK {
				ck.lastLeader = cur
				return
			}
		}

		DPrintf("Client %d, Retrying %s, %s, %s, Fail from %d, %s", ck.id, op, key, value, cur, reply.Err)

		cur++
		cur = cur % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
