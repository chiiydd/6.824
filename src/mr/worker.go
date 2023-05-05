package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

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

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task := getTask()
		switch task.TaskState {
		case Map:
			mapper(task, mapf)
		case Reduce:
			reducer(task, reducef)
		case Wait:
			time.Sleep(time.Second * 5)
		case Exit:
			return
		}

	}

}
func getTask() *Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.AssignTask", &args, &reply)
	return &reply
}
func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", task, &reply)
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

func writeToLocalFile(taskID int, index int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()

	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")

	if err != nil {
		log.Fatal("fail to  create temp file ", err)
	}

	enc := json.NewEncoder(tempFile)

	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("failed to write kv pair ", err)
		}

	}
	tempFile.Close()

	outputName := fmt.Sprintf("mr-%d-%d", taskID, index)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}
func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()

	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")

	if err != nil {
		log.Fatal("reducer fail to create tempfile ", err)
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}
func mapper(task *Task, mapf func(string, string) []KeyValue) {

	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("fail to read the file :"+task.Input, err)
	}

	intermediates := mapf(task.Input, string(content))

	buffer := make([][]KeyValue, task.NumReduce)

	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NumReduce
		buffer[slot] = append(buffer[slot], intermediate)
	}
	mapOutput := make([]string, 0)

	for i := 0; i < task.NumReduce; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskID, i, &buffer[i]))
	}

	task.Intermediates = mapOutput
	TaskCompleted(task)
}

func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}

	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open the local file ", err)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()

	}
	return &kva
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
