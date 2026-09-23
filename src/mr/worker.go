package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func launch_reduce(reducef func(string, []string) string, id int) {
	var kva []KeyValue
	matches, err := filepath.Glob(fmt.Sprintf("mr-*-%d", id))
	if err != nil {
		fmt.Printf("we encountred an error while searching for file")
		return
	}
	for _, filename := range matches {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("unable to open intermediate file %s", filename)
			continue
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

	sort.Sort(ByKey(kva))
	tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-out-temp-%d", id))
	if err != nil {
		fmt.Print("error while trying to create temp file")
		return
	}
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", id)
	os.Rename(tempFile.Name(), oname)
	CallReduceTaskDone(id)
}

func launch_map(mapf func(string, string) []KeyValue, task TaskData) {
	intermediate := mapf(task.Filename, task.FileContent)
	nReduce := task.NReduce

	bucket := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		BucketKey := ihash(kv.Key) % nReduce
		bucket[BucketKey] = append(bucket[BucketKey], kv)
	}
	var inames []string

	for idx, bkt := range bucket {
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-temp-%d-%d", task.TaskId, idx))
		if err != nil {
			fmt.Print("error while trying to create temp file")
			return
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range bkt {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Print("unable to write intermediage kv to json")
				return
			}
		}
		tempFile.Close()
		iname := fmt.Sprintf("mr-%d-%d", task.TaskId, idx)
		inames = append(inames, iname)
		os.Rename(tempFile.Name(), iname)
	}
	CallTaskDone(task, inames)

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var Nreduce int
	// Your worker implementation here.
	// Map task worker full loop
	for !CallMapCompleted() {
		task := CallGetMapTask()
		if Nreduce <= 0 {
			Nreduce = task.NReduce
		}
		if task.Filename == "" && task.FileContent == "" {
			time.Sleep(100 * time.Millisecond) // or 500ms

			continue
		}
		// wg.Add(1)
		func() {
			// defer wg.Done()
			launch_map(mapf, task)
		}()
	}
	// wg.Wait()

	// Reduce tasks worker implementation full loop
	for !CallReduceCompleted() {
		task := CallGetReduceTask()
		if task.ReducerId == -1 {
			time.Sleep(100 * time.Millisecond) // or 500ms
			continue
		}
		launch_reduce(reducef, task.ReducerId)
	}
}

func CallGetMapTask() TaskData {
	task := TaskData{}
	worker := MapWorker{}

	ok := call("Coordinator.GetMapTask", &worker, &task)

	if ok {
		fmt.Printf("got a worker task for %v", task.Filename)
	} else {
		fmt.Printf(("rpc call failed!\n"))
	}
	return task
}

func CallGetReduceTask() ReduceTask {
	task := ReduceTask{}
	ok := call("Coordinator.GetReduceTask", &GenericRPCRequest{}, &task)
	if ok {
		fmt.Println("got a reduce task", task.ReducerId)
	} else {
		fmt.Printf(("rpc call failed for get reduce task!\n"))
	}
	return task
}

func CallMapCompleted() bool {
	reply := MapDoneReply{}
	ok := call("Coordinator.IsMapDone", &GenericRPCRequest{}, &reply)
	if ok {
		fmt.Print("all map tasks have been completed")
	} else {
		fmt.Print("map completed rpc call failed")
	}
	return reply.IsDone
}

func CallReduceCompleted() bool {
	reply := ReduceDoneReply{}
	ok := call("Coordinator.IsReduceCompleted", &GenericRPCRequest{}, &reply)
	if ok {
		fmt.Print("all reduce tasks have been completed")
	} else {
		fmt.Print("map completed rpc call failed")
	}
	return reply.IsDone

}

func CallTaskDone(task TaskData, inames []string) {
	doneReply := CoordinatorReply{}
	request := WorkerDoneRequest{
		Task:   task,
		Inames: inames,
	}
	ok := call("Coordinator.WorkerDone", &request, &doneReply)
	if !ok {
		fmt.Print("error while callink worker done rpc")
	}

}

func CallReduceTaskDone(taskid int) {
	doneReply := CoordinatorReply{}
	request := ReduceTask{
		ReducerId: taskid,
	}
	ok := call("Coordinator.ReduceWorkerDone", &request, &doneReply)
	if !ok {
		fmt.Print("error while callink worker done rpc")
	}

}

func CallMarkAllDone() {
	ok := call("Coordinator.MarkAllDone", &GenericRPCRequest{}, &GenericRPCRequest{})
	if !ok {
		fmt.Print("error while marking all done")
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
