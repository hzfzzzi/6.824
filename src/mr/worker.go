package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 拿任务，先拿map，再拿reduce
	hasTask := true
	for hasTask {
		time.Sleep(time.Second)
		req := AskTaskRequest{}
		resp := AskTaskResponse{}
		if ok := call("Coordinator.AskTask", &req, &resp); !ok {
			fmt.Printf("call failed: ask task\n")
		}
		t := AskTaskResponse{}
		if t == resp {
			fmt.Print("maybe someone died, wait for 1s \n")
			continue
		}
		if !resp.HasTask {
			hasTask = false
			break
		}
		fmt.Printf("get task, type %v \n", resp.TaskType)
		switch resp.TaskType {
		case REDUCE:
			status, err := doReduce(&resp, reducef)
			if err != nil {
				fmt.Printf("do reduce failed")
			}
			reduceTaskDoneResp := ReduceTaskDoneResponse{}
			reduceTaskDoneReq := ReduceTaskDoneRequest{
				Status: status,
				Number: resp.Number,
			}
			if ok := call("Coordinator.ReduceTaskDone", &reduceTaskDoneReq, &reduceTaskDoneResp); !ok {
				fmt.Printf("call failed: reduce task done \n")
			}
		case MAP:
			status, err := doMap(&resp, mapf)
			if err != nil {
				fmt.Printf("do map failed: %s", resp.FilePath)
			}
			mapTaskDoneResp := MapTaskDoneRequest{}
			mapTaskDoneReq := MapTaskDoneRequest{
				FilePath: resp.FilePath,
				Status:   status,
			}
			if ok := call("Coordinator.MapTaskDone", &mapTaskDoneReq, &mapTaskDoneResp); !ok {
				fmt.Printf("call failed: map task done \n")
			}
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
		intermediateFileName := "mr-" + resp.FilePath[3:] + "-" + strconv.Itoa(i)
		fmt.Print(intermediateFileName)
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
	var fileList []string
	root := "/home/hzfzzzi/6.824/src/main/mr-tmp"

	files, err := ioutil.ReadDir(root)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		fmt.Print(f.Name())
		if ok := strings.HasSuffix(f.Name(), strconv.Itoa(resp.Number)); ok {
			fileList = append(fileList, f.Name())
		}
	}

	intermediate := []KeyValue{}
	for _, s := range fileList {
		f, err := os.Open(s)
		if err != nil {
			log.Fatalf("cannot open %v", s)
			return FAILED, err
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(resp.Number)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return FINISHED, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
