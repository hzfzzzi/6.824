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
	filesPath []string
	//intermediateFilesPath []string
	nReducer        int
	mapStatus       map[string]status
	reduceStatus    map[int]status
	mapStartTime    map[string]int64
	reduceStartTime map[int]int64
	mu              sync.Mutex
	hasTask         bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(req *AskTaskRequest, resp *AskTaskResponse) (err error) {
	if err != nil {
		fmt.Printf("ask task err")
		return err
	}
	hasMapTask, hasReduceTask := true, true
	if hasMapTask {
		hasMapTask = c.allocateMap(req, resp)
	}
	if !hasMapTask && hasReduceTask {
		hasReduceTask = c.allocateReduce(req, resp)
	}
	if !hasMapTask && !hasReduceTask {
		c.hasTask = false
	}
	return nil
}

func (c *Coordinator) allocateMap(req *AskTaskRequest, resp *AskTaskResponse) (hasTask bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for f, status := range c.mapStatus {
		if status == NOT_START {
			c.mapStatus[f] = PROCESSING
			c.mapStartTime[f] = time.Now().Unix()
			resp.FilePath = f
			resp.TaskType = MAP
			resp.NReduce = c.nReducer
			resp.HasTask = c.hasTask
			return true
		}
		if status == PROCESSING {
			t := time.Now().Unix()
			if t-c.mapStartTime[f] >= 10 {
				fmt.Printf("map task timeout!,re allocate it \n")
				c.mapStatus[f] = NOT_START
			}
			return true
		}
	}
	return false
}

func (c *Coordinator) allocateReduce(req *AskTaskRequest, resp *AskTaskResponse) (hasTask bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for num, status := range c.reduceStatus {
		if status == NOT_START {
			c.reduceStatus[num] = PROCESSING
			c.reduceStartTime[num] = time.Now().Unix()
			resp.Number = num
			resp.TaskType = REDUCE
			resp.HasTask = c.hasTask
			return true
		}
		if status == PROCESSING {
			t := time.Now().Unix()
			if t-c.reduceStartTime[num] >= 10 {
				fmt.Printf("reduce task timeout! \n")
				c.reduceStatus[num] = NOT_START
			}
			return true
		}
	}
	return false
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

func (c *Coordinator) ReduceTaskDone(req *ReduceTaskDoneRequest, resp *ReduceTaskDoneResponse) (err error) {
	if req.Status == FAILED {
		c.reduceStatus[req.Number] = NOT_START
	}
	if req.Status == FINISHED {
		c.reduceStatus[req.Number] = FINISHED
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if !c.hasTask {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filesPath:       files,
		nReducer:        nReduce,
		mapStatus:       make(map[string]status),
		reduceStatus:    make(map[int]status),
		mapStartTime:    make(map[string]int64),
		reduceStartTime: make(map[int]int64),
		hasTask:         true,
	}
	// Your code here.
	for _, f := range c.filesPath {
		c.mapStatus[f] = NOT_START
		c.mapStartTime[f] = time.Now().Unix()
	}
	for i := 0; i < c.nReducer; i++ {
		c.reduceStatus[i] = NOT_START
		c.reduceStartTime[i] = time.Now().Unix()
	}
	c.server()
	return &c
}
