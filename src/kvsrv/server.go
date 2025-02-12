package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kvmap  map[string]string
	record sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvmap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Type == Report {
		kv.record.Delete(args.MessageID)
		return
	}
	res, isExisted := kv.record.Load(args.MessageID)
	if isExisted {
		reply.Value = res.(string)
		return
	}
	kv.mu.Lock()
	old := kv.kvmap[args.Key]
	kv.kvmap[args.Key] = args.Value
	reply.Value = old
	kv.mu.Unlock()

	kv.record.Store(args.MessageID, old)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Type == Report {
		kv.record.Delete(args.MessageID)
		return
	}
	res, isExisted := kv.record.Load(args.MessageID)
	if isExisted {
		reply.Value = res.(string)
		return
	}

	kv.mu.Lock()
	oldV := kv.kvmap[args.Key]
	kv.kvmap[args.Key] += args.Value
	reply.Value = oldV
	kv.mu.Unlock()

	kv.record.Store(args.MessageID, oldV)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kvmap = make(map[string]string)
	return kv
}
