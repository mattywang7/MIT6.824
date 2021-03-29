package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusError   = 4
)

const (
	MaxTasksRunning  = 5 * time.Second
	ScheduleInterval = 500 * time.Millisecond
)

type TaskStatus struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	files        []string
	nReduce      int
	taskPhase    TaskPhase    // mu
	taskStatuses []TaskStatus // all TaskStatusReady initially, mu
	mu           sync.Mutex
	done         bool // true if all the tasks are finished, mu
	workerSeq    int
	taskCh       chan *Task // mu
}

func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskStatuses = make([]TaskStatus, len(c.files))
}

func (c *Coordinator) initReduceTask() {
	DPrintf("init reduce tasks")
	c.taskPhase = ReducePhase
	c.taskStatuses = make([]TaskStatus, c.nReduce)
}

func (c *Coordinator) getTask(taskSeq int) *Task {
	task := &Task{
		FileName: "",
		NReduce:  c.nReduce,
		NMap:     len(c.files),
		Seq:      taskSeq,
		Phase:    c.taskPhase,
		Alive:    true,
	}
	DPrintf("c: %+v, taskSeq: %d, lenFiles: %d, lenTs: %d", c, taskSeq, len(c.files), len(c.taskStatuses))
	if task.Phase == MapPhase {
		task.FileName = c.files[taskSeq]
	}
	return task
}

func (c *Coordinator) schedule() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		return
	}
	allFinish := true
	for index, ts := range c.taskStatuses {
		switch ts.Status {
		case TaskStatusReady:
			allFinish = false
			c.taskCh <- c.getTask(index)
			c.taskStatuses[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if time.Now().Sub(ts.StartTime) > MaxTasksRunning {
				c.taskStatuses[index].Status = TaskStatusQueue
				c.taskCh <- c.getTask(index)
			}
		case TaskStatusFinish:
			// nothing
		case TaskStatusError:
			allFinish = false
			c.taskStatuses[index].Status = TaskStatusQueue
			c.taskCh <- c.getTask(index)
		default:
			panic("ts.Status error")
		}
	}
	if allFinish {
		if c.taskPhase == MapPhase {
			c.initReduceTask()
		} else {
			c.done = true
		}
	}
}

// registerTask set the taskStatuses of that task in c
func (c *Coordinator) registerTask(args *TaskArgs, task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if task.Phase != c.taskPhase {
		panic("task phase not compatible")
	}

	c.taskStatuses[task.Seq].Status = TaskStatusRunning
	c.taskStatuses[task.Seq].WorkerId = args.WorkerId
	c.taskStatuses[task.Seq].StartTime = time.Now()
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-c.taskCh
	reply.Task = task

	if task.Alive {
		c.registerTask(args, task)
	}
	DPrintf("in get one task args: %v, reply: %v", args, reply)
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	DPrintf("get report task: %+v, taskPhase: %+v", args, c.taskPhase)

	if c.taskPhase != args.Phase || args.WorkerId != c.taskStatuses[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		c.taskStatuses[args.Seq].Status = TaskStatusFinish
	} else {
		c.taskStatuses[args.Seq].Status = TaskStatusError
	}

	go c.schedule()
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerSeq++
	reply.WorkerId = c.workerSeq
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *Coordinator) tickSchedule() {
	for !c.Done() {
		go c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.files = files
	if nReduce > len(c.files) {
		c.taskCh = make(chan *Task, nReduce)
	} else {
		c.taskCh = make(chan *Task, len(c.files))
	}

	c.initMapTask()
	go c.tickSchedule()

	c.server()
	DPrintf("Coordinator Made")
	return &c
}
