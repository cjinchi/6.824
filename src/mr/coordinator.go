package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// All the input files
	files []string
	// for map phase: the min index of the files that haven't been assigned to any worker
	// for reduce phase: the next reduce Id
	//index   int
	taskPool map[int]bool
	nReduce  int
	// num of workers
	nWorker   int
	phase     CoordinatorPhase
	doneTasks map[int]bool
	done      bool
	sync.Mutex
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.taskPool = make(map[int]bool)
	for index, _ := range files {
		c.taskPool[index] = true
	}
	c.nReduce = nReduce
	c.nWorker = 0
	c.phase = Map
	c.doneTasks = make(map[int]bool)

	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) RegisterHandler(args *RegisterArgs, reply *RegisterReply) error {
	c.Lock()
	defer c.Unlock()
	reply.Id = c.nWorker
	c.nWorker++
	reply.NReduce = c.nReduce
	reply.NFiles = len(c.files)
	return nil
}

func (c *Coordinator) RequestTaskHandler(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.Lock()
	defer c.Unlock()
	if len(c.taskPool) == 0 {
		// no more task in current phase
		reply.Assign = false
	} else {
		reply.Assign = true
		index := -1
		// get arbitrary task
		for k := range c.taskPool {
			index = k
			break
		}
		delete(c.taskPool, index)
		reply.Task.Id = index

		if c.phase == Map {
			reply.Task.Type = Map
			reply.Task.Filename = c.files[index]
		} else {
			reply.Task.Type = Reduce
			reply.Task.Id = index
		}

		go c.monitorTask(index, c.phase)
	}

	return nil
}

func (c *Coordinator) monitorTask(taskId int, phase CoordinatorPhase) {
	time.Sleep(10 * time.Second)
	c.Lock()
	defer c.Unlock()
	_, done := c.doneTasks[taskId]
	if phase == c.phase && !done {
		// add to pool again
		c.taskPool[taskId] = true
	}
}

func (c *Coordinator) TaskDoneHandler(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.Lock()
	defer c.Unlock()
	if c.phase == Map && args.TaskType == Map || c.phase == Reduce && args.TaskType == Reduce {
		c.doneTasks[args.TaskId] = true
	}
	if c.phase == Map && len(c.doneTasks) == len(c.files) {
		c.phase = Reduce
		c.taskPool = make(map[int]bool)
		for i := 0; i < c.nReduce; i++ {
			c.taskPool[i] = true
		}
		c.doneTasks = make(map[int]bool) // clear map
	}
	if c.phase == Reduce && len(c.doneTasks) == c.nReduce {
		c.done = true
	}
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
	// Your code here.
	c.Lock()
	defer c.Unlock()

	return c.done
}
