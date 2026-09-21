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
	"sync"
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

	oname := fmt.Sprintf("mr-out-%d", id)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

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
	var wg sync.WaitGroup
	for !CallMapCompleted() {
		task := CallGetMapTask()
		if Nreduce <= 0 {
			Nreduce = task.NReduce
		}
		if task.Filename == "" && task.FileContent == "" {
			time.Sleep(100 * time.Millisecond) // or 500ms

			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			launch_map(mapf, task)
		}()
	}
	wg.Wait()
	id := 0
	// Reduce tasks worker implementation full loop
	for id < Nreduce {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			launch_reduce(reducef, id)
		}(id)
		id++
	}
	wg.Wait()
	CallMarkAllDone()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
