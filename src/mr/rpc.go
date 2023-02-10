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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AskTaskRequest struct {
}

type AskTaskResponse struct {
	FilePath string
	Number   int
	TaskType taskType
	NReduce  int
	HasTask  bool
}

type MapTaskDoneRequest struct {
	Status   status
	FilePath string
}

type MapTaskDoneResponse struct {
}

type ReduceTaskDoneRequest struct {
	Status status
	Number int
}
type ReduceTaskDoneResponse struct {
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
