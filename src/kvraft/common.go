package kvraft

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrChanClose     = "ErrChanClose"
	ErrHandleTimeout = "ErrHandleTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op         string //表明是put 还是appedn
	Seq        uint64
	Identifier int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq        uint64
	Identifier int64
}

type GetReply struct {
	Err   Err
	Value string
}
