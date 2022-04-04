package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
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

type WorkerEntity struct {
	id      int
	nReduce int
	nFiles  int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
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

	w := WorkerEntity{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	for {
		ret, assign, task := w.requestTask()
		if !ret {
			// call failed since coordinator had exited
			break
		}
		if !assign {
			// no more Task in current phase
			time.Sleep(time.Second)
			continue
		}
		if task.Type == Map {
			w.executeMapTask(task.Id, task.Filename)
		} else if task.Type == Reduce {
			w.executeReduceTask(task.Id)
		}
	}
}

func (w *WorkerEntity) executeMapTask(taskId int, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := w.mapf(filename, string(content))

	buckets := make([][]KeyValue, w.nReduce)
	for i := 0; i < w.nReduce; i++ {
		buckets[i] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		index := ihash(kv.Key) % w.nReduce
		buckets[index] = append(buckets[index], kv)
	}

	for i := 0; i < w.nReduce; i++ {
		sort.Sort(ByKey(buckets[i]))
		oname := fmt.Sprintf("m-%v-%v", taskId, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
	}
	w.taskDone(Map, taskId)
}

func (w *WorkerEntity) executeReduceTask(reduceId int) {
	intermediate := []KeyValue{}
	for mapId := 0; mapId < w.nFiles; mapId++ {
		iname := fmt.Sprintf("m-%v-%v", mapId, reduceId)
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reduceId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	w.taskDone(Reduce, reduceId)
}

func (w *WorkerEntity) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}

	call("Coordinator.RegisterHandler", &args, &reply)

	w.id = reply.Id
	w.nReduce = reply.NReduce
	w.nFiles = reply.NFiles
}

func (w *WorkerEntity) requestTask() (bool, bool, Task) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	callResult := call("Coordinator.RequestTaskHandler", &args, &reply)
	return callResult, reply.Assign, reply.Task
}

func (w *WorkerEntity) taskDone(taskType TaskType, taskId int) {
	args := TaskDoneArgs{TaskType: taskType, TaskId: taskId}
	reply := TaskDoneReply{}
	call("Coordinator.TaskDoneHandler", &args, &reply)

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
