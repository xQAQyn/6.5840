package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Queue[T comparable] struct {
	list  *list.List
	mutex sync.Mutex
}

func NewQueue[T comparable]() *Queue[T] {
	return &Queue[T]{list: list.New()}
}

type entry[T comparable] struct {
	item T
	t    time.Time
}

func (q *Queue[T]) push(item T) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.list.PushBack(entry[T]{item: item, t: time.Now()})
}

func (q *Queue[T]) pop() T {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	ret := q.list.Front().Value.(entry[T]).item
	q.list.Remove(q.list.Front())
	return ret
}

func (q *Queue[T]) front() entry[T] {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.list.Front().Value.(entry[T])
}

func (q *Queue[T]) empty() bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.list.Len() == 0
}

func (q *Queue[T]) remove(value T) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for ele := q.list.Front(); ele != nil; {
		nxt := ele.Next()
		if ele.Value.(entry[T]).item == value {
			q.list.Remove(ele)
		}
		ele = nxt
	}
}

type Coordinator struct {
	mutex sync.Mutex

	// Your definitions here.
	MapWaitQueue       *Queue[string]
	MapProcessingQueue *Queue[string]

	ReduceWaitQueue       *Queue[int]
	ReduceProcessingQueue *Queue[int]
}

const retryOverlap = 10 * time.Second

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PullMap(args *PullMapArgs, reply *PullMapReply) error {
	// fmt.Printf("Receive new pull map task request\n")
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.MapWaitQueue.empty() {
		reply.Done = false
		reply.FileName = c.MapWaitQueue.pop()
		c.MapProcessingQueue.push(reply.FileName)
		// fmt.Printf("Deliver map task for %v\n", reply.FileName)
	} else if !c.MapProcessingQueue.empty() {
		reply.Done = false
		firstEntry := c.MapProcessingQueue.front()
		if time.Since(firstEntry.t) > retryOverlap {
			reply.FileName = c.MapProcessingQueue.pop()
			c.MapProcessingQueue.push(reply.FileName)
			// fmt.Printf("Deliver map task for %v\n", reply.FileName)
		} else {
			reply.FileName = ""
			// fmt.Printf("No waiting or timeout task\n")
		}
	} else {
		reply.Done = true
		// fmt.Printf("All map task are done\n")
	}
	return nil
}

func (c *Coordinator) SubmitMap(args *SubmitMapArgs, reply *SubmitMapReply) error {
	// fmt.Printf("Receive new submit map task request\n")
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.MapProcessingQueue.remove(args.FileName)
	// fmt.Printf("Map task for %v is submitted\n", args.FileName)

	return nil
}

func (c *Coordinator) PullReduce(args *PullReduceArgs, reply *PullReduceReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.ReduceWaitQueue.empty() {
		reply.Done = false
		reply.ReduceIdx = c.ReduceWaitQueue.pop()
		c.ReduceProcessingQueue.push(reply.ReduceIdx)
	} else if !c.ReduceProcessingQueue.empty() {
		reply.Done = false
		firstEntry := c.ReduceProcessingQueue.front()
		if time.Since(firstEntry.t) > retryOverlap {
			reply.ReduceIdx = c.ReduceProcessingQueue.pop()
			c.ReduceProcessingQueue.push(reply.ReduceIdx)
		} else {
			reply.ReduceIdx = -1
		}
	} else {
		reply.Done = true
	}
	return nil
}

func (c *Coordinator) SubmitReduce(args *SubmitReduceArgs, reply *SubmitReduceReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.ReduceProcessingQueue.remove(args.ReduceIdx)

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.MapWaitQueue.empty() && c.MapProcessingQueue.empty() &&
		c.ReduceWaitQueue.empty() && c.ReduceProcessingQueue.empty()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapWaitQueue:          NewQueue[string](),
		MapProcessingQueue:    NewQueue[string](),
		ReduceWaitQueue:       NewQueue[int](),
		ReduceProcessingQueue: NewQueue[int](),
	}

	// Your code here.
	for _, file := range files {
		c.MapWaitQueue.push(file)
	}

	for i := range reduceN {
		c.ReduceWaitQueue.push(i)
	}

	c.server(sockname)
	return &c
}
