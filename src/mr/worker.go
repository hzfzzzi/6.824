package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
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

	// 拿任务，先拿map，再拿reduce
	req := AskTaskRequest{}
	resp := AskTaskResponse{}
	if ok := call("Coordinator.AskTask", &req, &resp); !ok {
		fmt.Printf("call failed: ask task\n")
	}
	fmt.Printf("get task: %s", resp.FilePath)
	switch resp.TaskType {
	case REDUCE:

	case MAP:
		status, err := doMap(&resp, mapf)
		if err != nil {
			fmt.Printf("do map failed: %s,id: %v", resp.FilePath, resp.MapTaskID)
		}
		mapTaskDoneResp := MapTaskDoneRequest{}
		mapTaskDoneReq := MapTaskDoneRequest{
			FilePath:  resp.FilePath,
			MapTaskID: resp.MapTaskID,
			Status:    status,
		}
		if ok := call("Coordinator.MapTaskDone", &mapTaskDoneReq, &mapTaskDoneResp); !ok {
			fmt.Printf("call failed: map task done\n")
		}
	}

}

func doMap(resp *AskTaskResponse, mapf func(string, string) []KeyValue) (status status, err error) {
	file, err := os.Open(resp.FilePath)
	if err != nil {
		log.Fatalf("cannot open %v", resp.FilePath)
		return FAILED, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", resp.FilePath)
		return FAILED, err
	}
	file.Close()
	// here maybe crash
	kva := mapf(resp.FilePath, string(content))
	partitionedKva := make([][]KeyValue, resp.NReduce)
	for _, v := range kva {
		partitionKey := ihash(v.Key) % resp.NReduce
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}
	for i, kvs := range partitionedKva {
		intermediateFileName := "mr-" + strconv.Itoa(resp.MapTaskID) + "-" + strconv.Itoa(i)
		file, _ := os.Create(intermediateFileName)
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				fmt.Printf("encode kv error")
				return FAILED, err
			}
		}
	}
	return FINISHED, nil
}

func doReduce(resp *AskTaskResponse, reducef func(string, []string) string) (status status, err error) {
	return FINISHED, nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func AskTask() {

	// declare an argument structure.
	req := AskTaskRequest{}

	// fill in the argument(s).

	// declare a reply structure.
	resp := ExampleReply{}

	if ok := call("Coordinator.AskTask", &req, &resp); !ok {
		fmt.Printf("call failed!\n")
	}
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
