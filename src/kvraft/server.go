package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Type      string
	C         chan Result
	ClientId  int64
	RequestId int
}

type Result struct {
	Err       Err
	Value     string
	CmdIdx    int
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store          map[string]string   // store of the key/value
	channels       map[int]chan Result // store of channels
	lastRequest    map[int64]int       // store of last request id
	lastCommandIdx int
}

func (kv *KVServer) cleanChannels() {
	kv.channels = make(map[int]chan Result)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:       args.Key,
		Type:      "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.channels[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.channels[idx] = ch
	}
	kv.mu.Unlock()

	select {
	case <-time.After(600 * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	case res := <-ch:
		_, isLeader := kv.rf.GetState()
		if res.ClientId != op.ClientId || res.RequestId != op.RequestId || !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		reply.Value = res.Value
	}

	kv.mu.Lock()
	delete(kv.channels, idx)
	kv.mu.Unlock()

	// DPrintf("%d Start waiting for %s, %s, %s", kv.me, op.Key, op.Value, op.Type)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch, ok := kv.channels[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.channels[idx] = ch
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * 600):
		reply.Err = ErrWrongLeader
	case res := <-ch:
		if res.ClientId != op.ClientId || res.RequestId != op.RequestId {
			reply.Err = ErrWrongLeader
			return
		}

		reply.Err = OK
		kv.mu.Lock()
		delete(kv.channels, idx)
		kv.mu.Unlock()
	}
	return
	// DPrintf("Start waiting for client %d, %s, %s, %s", op.ClientId, op.Key, op.Value, op.Type)
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

func (kv *KVServer) snapshotting() {

	for {
		time.Sleep(100 * time.Millisecond)
		if kv.rf.GetRaftSize() >= kv.maxraftstate {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.store)
			e.Encode(kv.lastRequest)
			e.Encode(kv.lastCommandIdx)
			data := w.Bytes()
			lastCommandIdx := kv.lastCommandIdx
			kv.mu.Unlock()

			DPrintf("server %d start to compact log %d", kv.me, lastCommandIdx)
			kv.rf.StartSnapshot(data, lastCommandIdx)
		}
	}
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.channels = make(map[int]chan Result)
	kv.lastRequest = make(map[int64]int)
	time.Sleep(300 * time.Millisecond)

	// start the background thread to listent to the applyCh
	// to get the committed op and mutate the kv store
	go func() {
		for msg := range kv.applyCh {
			if msg.CommandValid {

				op := msg.Command.(Op)
				idx := msg.CommandIndex
				res := Result{
					Err:       "",
					Value:     "",
					CmdIdx:    msg.CommandIndex,
					ClientId:  op.ClientId,
					RequestId: op.RequestId,
				}
				// start to handle the committed op
				kv.mu.Lock()
				// handle the duplicated request, by checking the request id
				lastId := kv.lastRequest[op.ClientId]
				kv.lastCommandIdx = idx
				if op.RequestId > lastId {
					kv.lastRequest[op.ClientId] = op.RequestId
					if op.Type == "Get" {
						val, ok := kv.store[op.Key]
						if ok {
							res.Value = val
							res.Err = OK
						} else {
							res.Err = ErrNoKey
						}
					} else if op.Type == "Put" {
						kv.store[op.Key] = op.Value
						res.Err = OK
					} else {
						val, ok := kv.store[op.Key]
						if ok {
							kv.store[op.Key] = val + op.Value
						} else {
							kv.store[op.Key] = op.Value
						}
						res.Err = OK
						val, ok = kv.store[op.Key]
					}
				} else {
					res.Err = OK
					if op.Type == "Get" {
						val, ok := kv.store[op.Key]
						if ok {
							res.Value = val
						} else {
							res.Err = ErrNoKey
						}
					}
				}

				ch, ok := kv.channels[idx]
				if !ok {
					ch = make(chan Result, 1)
					kv.channels[idx] = ch
				}
				kv.mu.Unlock()
				// DPrintf("Finish processing one result %s, %s, %s, client %d, request %d, server %d", op.Type, op.Key, op.Value, op.ClientId, op.RequestId, kv.me)

				ch <- res
			} else {
				data := msg.Command.([]byte)
				r := bytes.NewBuffer(data)
				d := labgob.NewDecoder(r)
				var store map[string]string
				var lastRequest map[int64]int
				var lastCommandIdx int
				if d.Decode(&store) != nil ||
					d.Decode(&lastRequest) != nil ||
					d.Decode(&lastCommandIdx) != nil {
					DPrintf("Fail to read kv store value")
				} else {
					kv.mu.Lock()
					kv.store = store
					kv.lastRequest = lastRequest
					kv.lastCommandIdx = lastCommandIdx
					DPrintf("server %d read snapshot, last command idx %d, installed value %v", kv.me, lastCommandIdx, kv.store)
					kv.mu.Unlock()
				}
			}
		}
	}()

	if kv.maxraftstate != -1 {
		go kv.snapshotting()
	}

	return kv
}
