package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu            sync.Mutex        // 互斥锁
	mapTasks      []Task            // Map 任务列表
	reduceTasks   []Task            // Reduce 任务列表
	taskStatus    map[int]bool      // 任务状态（是否完成）
	taskStartTime map[int]time.Time // 任务开始时间（用于超时检测）
	nReduce       int               // Reduce 任务的数量
}

type Task struct {
	Type       TaskType
	TaskID     int
	InputFiles []string
	NReduce    int
	OutputFile string
	FilesNum   int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 分配 Map 任务
	for _, task := range c.mapTasks {
		if !c.taskStatus[task.TaskID] {
			reply.Type = MapTask
			reply.TaskID = task.TaskID
			reply.InputFiles = task.InputFiles
			reply.NReduce = c.nReduce
			reply.FilesNum = task.FilesNum
			c.taskStatus[task.TaskID] = true
			c.taskStartTime[task.TaskID] = time.Now()
			return nil
		}
	}

	// 如果所有 Map 任务已完成，分配 Reduce 任务
	if c.allMapTasksDone() {
		for _, task := range c.reduceTasks {
			if !c.taskStatus[task.TaskID] {
				reply.Type = ReduceTask
				reply.TaskID = task.TaskID
				reply.InputFiles = task.InputFiles
				reply.NReduce = c.nReduce
				reply.OutputFile = task.OutputFile
				reply.FilesNum = task.FilesNum
				c.taskStatus[task.TaskID] = true
				c.taskStartTime[task.TaskID] = time.Now()
				return nil
			}
		}
	}

	// 如果所有任务已完成，通知 Worker 退出
	if c.Done() {
		reply.Type = ExitTask
		return nil
	}

	// 如果没有任务可分配，返回 NoTask
	reply.Type = NoTask
	return nil
}

// TaskDone 处理 Worker 完成任务的通知
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.taskStatus[args.TaskID] = true
	reply.Done = true
	return nil
}

// 检查所有 Map 任务是否已完成
func (c *Coordinator) allMapTasksDone() bool {
	for _, task := range c.mapTasks {
		if !c.taskStatus[task.TaskID] {
			return false
		}
	}
	return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// Done 检查所有任务是否已完成
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, done := range c.taskStatus {
		if !done {
			return false
		}
	}
	return true
}
func (c *Coordinator) checkTimeout() {
	for {
		c.mu.Lock()
		for taskID, assigned := range c.taskStatus {
			if assigned && time.Since(c.taskStartTime[taskID]) > 10*time.Second {
				c.taskStatus[taskID] = false // 重新分配任务
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:      make([]Task, len(files)),
		reduceTasks:   make([]Task, nReduce),
		taskStatus:    make(map[int]bool),
		taskStartTime: make(map[int]time.Time),
		nReduce:       nReduce,
	}

	// 初始化 Map 任务
	for i, file := range files {
		c.mapTasks[i] = Task{
			Type:       MapTask,
			TaskID:     i,
			InputFiles: []string{file},
			NReduce:    nReduce,
			FilesNum:   len(files),
		}
		c.taskStatus[i] = false
	}

	// 初始化 Reduce 任务
	for i := 0; i < nReduce; i++ {
		inputFiles := []string{}
		for j := 0; j < len(files); j++ {
			inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", j, i))
		}
		c.reduceTasks[i] = Task{
			Type:       ReduceTask,
			TaskID:     i + len(files),
			InputFiles: inputFiles,
			NReduce:    nReduce,
			OutputFile: "",
			FilesNum:   len(files),
		}
		c.taskStatus[len(files)+i] = false
	}

	// 启动超时检测
	go c.checkTimeout()

	c.server()
	return &c
}
