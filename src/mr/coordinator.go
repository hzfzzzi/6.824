package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type status int

const (
	NOT_START  status = 0
	PROCESSING status = 1
	FINISHED   status = 2
	FAILED     status = 3
)

type taskType int

const (
	MAP    taskType = 1
	REDUCE taskType = 2
)

type Coordinator struct {
	// Your definitions here.
	filesPath         []string
	nReducer          int
	mapStatus         map[string]status
	mapTaskID         int
	reduceStatus      map[int]status
	intermediateFiles map[int][]string
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(req *AskTaskRequest, resp *AskTaskResponse) (err error) {
	if err != nil {
		fmt.Printf("ask task err")
		return err
	}
	fmt.Printf("ask task success \n")
	// 分配任务，如果还有map任务需要分配，则优先分配map任务，否则reduce任务
	for f, status := range c.mapStatus {
		if status == NOT_START {
			c.mu.Lock()
			c.mapStatus[f] = PROCESSING
			resp.FilePath = f
			resp.TaskType = MAP
			resp.MapTaskID = c.mapTaskID
			c.mapTaskID += 1
			resp.NReduce = c.nReducer
			c.mu.Unlock()
			break
		}
	}

	return nil
}

func (c *Coordinator) MapTaskDone(req *MapTaskDoneRequest, resp *MapTaskDoneResponse) (err error) {
	if req.Status == FAILED {
		c.mapStatus[req.FilePath] = NOT_START
	}
	if req.Status == FINISHED {
		c.mapStatus[req.FilePath] = FINISHED
	}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filesPath: files,
		nReducer:  nReduce,
		mapStatus: make(map[string]status),
		mapTaskID: 0,
	}

	// Your code here.
	for _, f := range c.filesPath {
		c.mapStatus[f] = NOT_START
	}
	c.server()
	return &c
}
