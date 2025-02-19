package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seq        uint64 //单调递增的序列号，标记请求
	identifier int64  //表示clerk，所以(identifier, seq)一起标记了唯一的请求
	leaderID   int    //记录leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) GetSeq() uint64 {
	ck.seq = uint64(nrand())
	return ck.seq
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &GetArgs{
		Key:        key,
		Seq:        ck.GetSeq(),
		Identifier: ck.identifier,
	}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID += 1
			ck.leaderID %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrHandleTimeout:
			continue
		case ErrNoKey:
			return ""
		}

		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:        key,
		Value:      value,
		Op:         op,
		Seq:        ck.GetSeq(),
		Identifier: ck.identifier,
	}
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID += 1
			ck.leaderID %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrHandleTimeout:
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
