package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

/*
rpc是remote procedure call，它是一种应用层自定义协议，其设计的想法就是我们像调用本地方法一样调用远程服务器提供的方法。

要实现rpc通信，客户端与服务端需要通过预先定义好的结构来进行通信
1.客户端调用服务端时发送的参数结构
2.服务端返回客户端的回复结构
*/

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

// TaskRequest 是 Worker 请求任务时的参数
type TaskRequest struct {
	WorkerID int // Worker 的唯一标识
}

// TaskReply 是 Coordinator 返回任务时的响应
type TaskReply struct {
	Type       TaskType // 任务类型
	TaskID     int      // 任务 ID
	InputFiles []string // 输入文件（Map 任务使用）
	NReduce    int      // Reduce 任务的数量
	OutputFile string   // 输出文件（Reduce 任务使用）
	FilesNum   int
}

// TaskDoneArgs 是 Worker 完成任务时的参数
type TaskDoneArgs struct {
	TaskID int // 完成的任务 ID
}

// TaskDoneReply 是 Coordinator 对任务完成的响应
type TaskDoneReply struct {
	Done bool // 任务是否成功完成
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
