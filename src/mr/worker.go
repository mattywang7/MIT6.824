package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
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
// set the mapf and reducef function
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

	// store the intermediate files in reduces[][]
	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		index := ihash(kv.Key) % t.NReduce
		reduces[index] = append(reduces[index], kv)
	}

	// create an intermediate file for each index in reduces[][]
	for index, line := range reduces {
		fileName := reduceName(t.Seq, index) // t.Seq = mapId, index = reduceId
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		encoder := json.NewEncoder(f)
		for _, kv := range line {
			if err := encoder.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
				// not return, get an uncompleted intermediate file
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t *Task) {
	// maps store key string, values []string
	maps := make(map[string][]string)
	// read NMap intermediate files from map workers
	for index := 0; index < t.NMap; index++ {
		// each file contain []KeyValue with kvs having the same Key
		fileName := reduceName(index, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v: %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}
	w.reportTask(t, true, nil)
}

// helper function reportTask
func (w *worker) reportTask(t *Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}

	args := ReportTaskArgs{}
	args.Done = done
	args.Phase = t.Phase
	args.WorkerId = w.id
	args.Seq = t.Seq
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail: %+v", args)
	}
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
