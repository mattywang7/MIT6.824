package mr

import (
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// worker definition
type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) run() {
	for {
		t := w.reqTask()
		if !t.Alive {
			DPrintf("Worker get task not alive, exit")
			return
		}
		w.doTask(t)
	}
}

func (w *worker) reqTask() *Task {
	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}

	if ok := call("Coordinator.GetOneTask", &args, &reply); !ok {
		DPrintf("worker get task failed, exit")
		os.Exit(1)
	}
	DPrintf("worker get task:%+v", reply.Task)
	return reply.Task
}

func (w *worker) doTask(t *Task) {
	DPrintf("doing task...")

	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase err: %v", t.Phase))
	}
}

func (w *worker) doMapTask(t *Task) {
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
	}
}

func (w *worker) doReduceTask(t *Task) {
	// TODO
}

// helper function reportTask
func (w *worker) reportTask(t *Task, done bool, err error) {
	// TODO
}

// register ask the Coordinator to get a workerId
func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Coordinator.RegisterWorker", args, reply); !ok {
		log.Fatal("Register fail")
	}
	w.id = reply.WorkerId
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
