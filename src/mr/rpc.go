package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

// Add your RPC definitions here.
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Assign bool
	Task   Task
}

//type RequestStateArgs struct {
//}
//
//type RequestStateReply struct {
//	NReduce int
//	mapDone bool
//	done    bool
//}

type RegisterArgs struct {
}

type RegisterReply struct {
	Id      int
	NReduce int
	NFiles  int
}

type TaskDoneArgs struct {
	TaskType TaskType
	TaskId   int
}

type TaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
