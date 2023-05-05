package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	Input         string
	TaskState     State
	NumReduce     int
	TaskID        int
	Intermediates []string
	Output        string
}

type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type Coordinator struct {
	// Your definitions here.
	TaskQueue        chan *Task
	TaskMeta         map[int]*CoordinatorTask
	CoordinatorPhase State
	NumberReduce     int
	InputFiles       []string
	Intermediates    [][]string
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

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

	mu.Lock()
	ret := c.CoordinatorPhase == Exit
	defer mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(len(files), nReduce)),
		CoordinatorPhase: Map,
		TaskMeta:         make(map[int]*CoordinatorTask),
		NumberReduce:     nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}

	// Your code here.
	c.createMapTask()
	go c.catchTimeOut()

	c.server()
	return &c
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		//defer mu.Unlock()
		if c.CoordinatorPhase == Exit {
			mu.Unlock()
			return
		}
		for _, task := range c.TaskMeta {
			if task.TaskStatus == InProgress && time.Now().Sub(task.StartTime) > 10.0*time.Second {
				c.TaskQueue <- task.TaskReference
				task.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) createMapTask() {
	for index, filename := range c.InputFiles {
		taskMeta := Task{
			Input:     filename,
			TaskState: Map,
			NumReduce: c.NumberReduce,
			TaskID:    index,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[index] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTask)
	for index, files := range c.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NumReduce:     c.NumberReduce,
			TaskID:        index,
			Intermediates: files,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[index] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}

	}
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskID].TaskStatus = InProgress
		c.TaskMeta[reply.TaskID].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()

	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskID].TaskStatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskID].TaskStatus = Completed
	go c.ProcessTaskResult(task)
	return nil

}

func (c *Coordinator) ProcessTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()

	switch task.TaskState {
	case Map:
		for reduceTaskID, filepath := range task.Intermediates {
			c.Intermediates[reduceTaskID] = append(c.Intermediates[reduceTaskID], filepath)
		}
		if c.AllDone() {
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.AllDone() {
			c.CoordinatorPhase = Exit
		}
	}
}

func (c *Coordinator) AllDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
