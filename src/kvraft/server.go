package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ApplyMsgTimeout = 1 * time.Second
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Id    uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state      map[string]string
	chanPool   map[uint64]chan bool
	applyState map[uint64]ApplyState
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{Type: "Get", Key: args.Key, Id: rand.Uint64()}
	kv.lock("Get 1")
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		//fmt.Println("wait for lock in Get1")
		//kv.mu.Lock()
		//fmt.Println("get lock in Get1")

		kv.chanPool[op.Id] = make(chan bool, 1)
		kv.applyState[op.Id] = WaitForApply
		//kv.mu.Unlock()
		kv.unlock("Get 1")
		timer := time.NewTimer(ApplyMsgTimeout)
		//fmt.Printf("%v start select on %v\n", kv.me, op.Id)
		select {
		case <-kv.chanPool[op.Id]:
			//fmt.Printf("%v get chan on %v\n", kv.me, op.Id)
		case <-timer.C:
			//fmt.Printf("%v get timeout on %v\n", kv.me, op.Id)
		}
		//fmt.Println("wait for lock in Get2")
		//kv.mu.Lock()
		//fmt.Println("get lock in Get2")
		kv.lock("Get 2")
		// check whether applied
		if kv.applyState[op.Id] != Applied {
			kv.applyState[op.Id] = Expired
			//kv.mu.Unlock()
			kv.unlock("Get 2")
			kv.Get(args, reply)
			return
		}

		if val, ok := kv.state[op.Key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		//kv.mu.Unlock()

	}
	kv.unlock("Get 3")
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Printf("1:%v\n", time.Now())
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, Id: rand.Uint64()}
	//fmt.Println("server enter putappend")
	//fmt.Println("wait for lock in PutAppend1")
	//kv.mu.Lock()
	//fmt.Println("get lock in PutAppend1")
	kv.lock("PutAppend 1")
	_, _, isLeader := kv.rf.Start(op)
	//fmt.Println("server done start in putappend")
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		timer := time.NewTimer(ApplyMsgTimeout)
		ch := make(chan bool, 1)
		kv.chanPool[op.Id] = ch
		kv.applyState[op.Id] = WaitForApply
		//kv.mu.Unlock()
		kv.unlock("PutAppend 0")
		//fmt.Printf("2:%v\n", time.Now())
		select {
		case <-ch:
			//fmt.Printf("%v putappend chan on %v\n", kv.me, op.Id)
		case <-timer.C:
			fmt.Printf("%v putappend timeout on %v\n", kv.me, op.Id)
		}
		//fmt.Printf("3:%v\n", time.Now())

		//fmt.Println("wait for lock in PutAppend2")
		//kv.mu.Lock()
		//fmt.Println("get lock in PutAppend2")
		kv.lock("PutAppend 2")
		// check whether applied
		if kv.applyState[op.Id] != Applied {
			kv.applyState[op.Id] = Expired
			kv.unlock("PutAppend 1")
			//fmt.Println("release in PutAppend1")
			//kv.mu.Unlock()
			kv.PutAppend(args, reply)
			return
		}
		reply.Err = OK
		//fmt.Printf("4:%v\n", time.Now())

	}
	//fmt.Println("release in PutAppend2")
	//kv.mu.Unlock()
	//fmt.Println("server done all in putappend")
	kv.unlock("PutAppend 2")
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.chanPool = make(map[uint64]chan bool)
	kv.applyState = make(map[uint64]ApplyState)

	go kv.applyLogs()

	return kv
}

func (kv *KVServer) applyLogs() {
	for {
		msg := <-kv.applyCh
		op, _ := (msg.Command).(Op)
		//fmt.Println("wait for lock in applyLogs")
		//kv.mu.Lock()
		//fmt.Println("get lock in applyLogs")
		fmt.Printf("%v try to get state\n", kv.me)
		_, isLeader := kv.rf.GetState()
		fmt.Printf("%v end get state\n", kv.me)

		kv.lock(fmt.Sprintf("applyLogs %v", op.Id))

		if isLeader && kv.applyState[op.Id] == WaitForApply {
			kv.applyState[op.Id] = Applied
			kv.chanPool[op.Id] <- true
		}

		//fmt.Printf("%v try to write to chan\n", kv.me)
		//
		//fmt.Printf("%v end write to chan\n", kv.me)
		fmt.Printf("%v apply %v\n", kv.me, op.Id)
		if op.Type == "Put" {
			kv.state[op.Key] = op.Value
		} else if op.Type == "Append" {
			if val, ok := kv.state[op.Key]; ok {
				kv.state[op.Key] = val + op.Value
			} else {
				kv.state[op.Key] = op.Value
			}
		}
		//kv.mu.Unlock()
		kv.unlock(fmt.Sprintf("applyLogs 2 %v", op.Id))
	}
}

func (kv *KVServer) lock(msg string) {
	fmt.Printf("%v wait for lock: %v\n", kv.me, msg)
	kv.mu.Lock()
	fmt.Printf("%v acquire lock: %v\n", kv.me, msg)
}

func (kv *KVServer) unlock(msg string) {
	fmt.Printf("%v unlock: %v\n", kv.me, msg)
	kv.mu.Unlock()
}
