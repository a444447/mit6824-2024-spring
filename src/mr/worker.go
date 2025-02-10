package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	workerID := os.Getpid()
	for {
		reply := CallAssignTask(workerID)
		switch reply.Type {
		case MapTask:
			doMapTask(mapf, reply)
		case ReduceTask:
			doReduceTask(reducef, reply)
		case ExitTask:
			return
		case NoTask:
			time.Sleep(time.Second)
		}
	}

}

func doMapTask(mapf func(string, string) []KeyValue, reply *TaskReply) {
	// 读取输入文件
	intermediate := []KeyValue{}
	for _, filename := range reply.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	buckets := make([][]KeyValue, reply.NReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kva := range intermediate {
		buckets[ihash(kva.Key)%reply.NReduce] = append(buckets[ihash(kva.Key)%reply.NReduce], kva)
	}

	for i := range buckets {
		oname := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i+reply.FilesNum)
		ofile, _ := os.CreateTemp("", oname+"*")
		enc := json.NewEncoder(ofile)
		for _, kva := range buckets[i] {
			err := enc.Encode(&kva)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}

	// 通知 Coordinator 任务完成
	args := TaskDoneArgs{TaskID: reply.TaskID}
	doneReply := TaskDoneReply{}
	call("Coordinator.TaskDone", &args, &doneReply)
}

// 执行 Reduce 任务
func doReduceTask(reducef func(string, []string) string, reply *TaskReply) {
	kva := []KeyValue{}
	for mapID := 0; mapID < len(reply.InputFiles); mapID++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", mapID, reply.TaskID))
		if err != nil {
			log.Fatalf("reduce/cannot open %v", fmt.Sprintf("mr-%d-%d", mapID, reply.TaskID))
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 按 Key 排序
	sort.Sort(ByKey(kva))

	// 调用 reducef 函数
	output := []KeyValue{}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output = append(output, KeyValue{kva[i].Key, reducef(kva[i].Key, values)})
		i = j
	}

	// 将结果写入输出文件
	file, err := ioutil.TempFile("", "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	for _, kv := range output {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	file.Close()
	os.Rename(file.Name(), fmt.Sprintf("mr-out-%d", reply.TaskID))

	// 通知 Coordinator 任务完成
	args := TaskDoneArgs{TaskID: reply.TaskID}
	doneReply := TaskDoneReply{}
	call("Coordinator.TaskDone", &args, &doneReply)
}

func CallAssignTask(workerID int) *TaskReply {
	args := TaskRequest{WorkerID: workerID}
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		log.Fatal("call assign task rpc error")
	}
	return &reply

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
