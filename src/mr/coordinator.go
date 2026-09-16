package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	files []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerDone(task *TaskData, reply *CoordinatorReply) error {
	c.files = removeByValueFast(c.files, task.Filename)
	return nil
}

// Get task RPC handler
func (c *Coordinator) GetMapTask(worker *MapWorker, task *TaskData) error {
	task.Filename = c.files[len(c.files)-1]
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
		return err
	}
	defer file.Close()

	contentBytes, err := io.ReadAll(file)

	if err != nil {
		log.Fatalf("failed to read file: %v", err)
		return err
	}

	content := string(contentBytes)
	task.FileContent = content

	fmt.Printf("assigned file %v", task.Filename)
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
	if len(c.files) == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.

	c.files = files

	// for _, fn := range files {
	// 	fmt.Println(fn)
	// }

	c.server()
	return &c
}

func removeByValueFast(slice []string, value string) []string {
	for i, v := range slice {
		if v == value {
			// Swap with last element and slice off the last element
			slice[i] = slice[len(slice)-1]
			return slice[:len(slice)-1]
		}
	}
	return slice
}
