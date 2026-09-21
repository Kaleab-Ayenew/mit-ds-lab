package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type mapFile struct {
	Name       string
	Locked     bool
	AcquiredAt time.Time
	TaskId     int
}

type Coordinator struct {
	// Your definitions here.
	files              []mapFile
	TaskCounter        SafeCounter
	ActiveMapWorkers   SafeCounter
	MaxMapWorkers      int
	ReduceWorkers      int
	WorkerCounter      SafeCounter
	IntermediateFiles  []string
	MapTaskLockTimeout time.Duration
	IsReduceCompleted  bool
	mu                 sync.Mutex
}

type SafeCounter struct {
	mu    sync.Mutex
	value int
}

func (c *SafeCounter) Inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
}

func (c *SafeCounter) Dec() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value--
}

func (c *SafeCounter) Val() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerDone(request *WorkerDoneRequest, reply *CoordinatorReply) error {
	c.mu.Lock()
	c.files = removeMapFileByName(c.files, request.Task.Filename)
	c.mu.Unlock()
	c.IntermediateFiles = append(c.IntermediateFiles, request.Inames...)
	c.ActiveMapWorkers.Dec()
	return nil
}

func (c *Coordinator) assignMapFile(TaskId int) (string, bool) {
	now := time.Now()
	for i := range c.files {
		f := &c.files[i]
		if !f.Locked {
			f.Locked = true
			f.AcquiredAt = now
			f.TaskId = TaskId
			return f.Name, true
		}
		if now.Sub(f.AcquiredAt) > c.MapTaskLockTimeout {
			f.AcquiredAt = now
			return f.Name, true
		}
	}
	return "", false
}

// Get task RPC handler
func (c *Coordinator) GetMapTask(worker *MapWorker, task *TaskData) error {
	if c.ActiveMapWorkers.Val() >= c.MaxMapWorkers {
		return nil
	}
	c.mu.Lock()
	filename, ok := c.assignMapFile(c.TaskCounter.value)
	task.TaskId = c.TaskCounter.value
	c.TaskCounter.Inc()
	c.mu.Unlock()
	if !ok {
		return nil
	}
	task.Filename = filename
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
	task.NReduce = c.ReduceWorkers
	task.WorkerId = c.WorkerCounter.value
	c.WorkerCounter.Inc()
	c.ActiveMapWorkers.Inc()

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
	return c.IsReduceCompleted
}

func (c *Coordinator) MarkAllDone(request *GenericRPCRequest, response *GenericRPCRequest) error {
	c.IsReduceCompleted = true
	return nil
}

func (c *Coordinator) IsMapDone(question *GenericRPCRequest, reply *MapDoneReply) error {
	c.mu.Lock()
	reply.IsDone = len(c.files) == 0
	c.mu.Unlock()
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapFiles := make([]mapFile, len(files))
	for i, name := range files {
		mapFiles[i] = mapFile{Name: name}
	}

	c := Coordinator{
		ActiveMapWorkers:   SafeCounter{value: 0},
		MaxMapWorkers:      10,
		files:              mapFiles,
		ReduceWorkers:      nReduce,
		WorkerCounter:      SafeCounter{value: 1},
		MapTaskLockTimeout: time.Minute,
		TaskCounter:        SafeCounter{value: 0},
	}

	// for _, fn := range files {
	// 	fmt.Println(fn)
	// }

	c.server()
	return &c
}

func removeMapFileByName(slice []mapFile, name string) []mapFile {
	for i, v := range slice {
		if v.Name == name {
			slice[i] = slice[len(slice)-1]
			return slice[:len(slice)-1]
		}
	}
	return slice
}
