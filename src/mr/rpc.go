package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type PullMapArgs struct {
}

type PullMapReply struct {
	FileName string
	Done     bool
}

type SubmitMapArgs struct {
	FileName string
}

type SubmitMapReply struct {
}

type PullReduceArgs struct {
}

type PullReduceReply struct {
	ReduceIdx int
	Done      bool
}

type SubmitReduceArgs struct {
	ReduceIdx int
}

type SubmitReduceReply struct {
}
