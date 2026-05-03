package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

const reduceN = 10

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func atomicWriteFile(targetFile string, kva []KeyValue, jsonFormat bool) error {
	temp_dir := "temp"
	if err := os.MkdirAll(temp_dir, 0755); err != nil {
		fmt.Printf("Create temp file dir fail: %v\n", err)
		return err
	}

	var tempFile, err = os.CreateTemp("temp", "*.txt")
	if err != nil {
		fmt.Printf("Create temp file fail: %v\n", err)
		return err
	}

	tempName := tempFile.Name()
	defer func() {
		if err != nil {
			os.Remove(tempName)
		}
	}()

	if jsonFormat {
		enc := json.NewEncoder(tempFile)

		for _, kv := range kva {
			if err := enc.Encode(kv); err != nil {
				tempFile.Close()
				fmt.Printf("write to temp file error: %v\n", err)
				return err
			}
		}
	} else {
		for _, kv := range kva {
			if _, err := fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value); err != nil {
				tempFile.Close()
				fmt.Printf("write to temp file error: %v\n", err)
				return err
			}
		}
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		fmt.Printf("temp file sync error: %v\n", err)
		return err
	}

	if err := tempFile.Close(); err != nil {
		fmt.Printf("temp file close error: %v\n", err)
		return err
	}

	if err := os.MkdirAll(filepath.Dir(targetFile), 0755); err != nil {
		return fmt.Errorf("create target dir: %w", err)
	}
	if err := os.Rename(tempName, targetFile); err != nil {
		fmt.Printf("rename error: %v\n", err)
		return err
	}

	return nil
}

func readIntermiateFile(fileName string, kva map[string][]string) error {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("intermediate file %v open failed: %v\n", fileName, err)
		return err
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("intermediate file %v decode error: %v\n", fileName, err)
			return err
		}
		kva[kv.Key] = append(kva[kv.Key], kv.Value)
	}

	return nil
}

func listFilesWithSuffix(dir, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), suffix) {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	return files, nil
}

func FileNameWithoutExt(fullPath string) string {
	fileName := filepath.Base(fullPath)
	ext := filepath.Ext(fileName)
	return strings.TrimSuffix(fileName, ext)
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// take map task if map is not finished
mapPhase:
	for true {
		fileName, done := CallPullMap()
		if done {
			break
		} else if fileName == "" {
			time.Sleep(5 * time.Second)
			continue
		} else {
			content, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Printf("Read file failed: %v\n", err)
				continue
			}
			kva := mapf(fileName, string(content))

			bucket := make([][]KeyValue, reduceN)
			for _, kv := range kva {
				idx := ihash(kv.Key) % reduceN
				bucket[idx] = append(bucket[idx], kv)
			}

			for i := 0; i < reduceN; i++ {
				targetFileName := fmt.Sprintf("intermediate/map_%v_reduce_%v.txt", FileNameWithoutExt(fileName), i)
				if err := atomicWriteFile(targetFileName, bucket[i], true); err != nil {
					fmt.Printf("Write intermediate file error: %v\n", err)
					continue mapPhase
				}
			}

			CallSubmitMap(fileName, kva)
		}
	}

	// take reduce task if reduce is not finished
	for true {
		reduceIdx, done := CallPullReduce()
		if done {
			break
		} else if reduceIdx == -1 {
			time.Sleep(5 * time.Second)
			continue
		} else {
			suffix := fmt.Sprintf("reduce_%v.txt", reduceIdx)
			fileNames, err := listFilesWithSuffix("intermediate", suffix)
			if err != nil {
				fmt.Printf("get map file name for reduce index %v failed: %v\n", reduceIdx, err)
				continue
			}
			kva := make(map[string][]string)
			for _, fileName := range fileNames {
				readIntermiateFile(fileName, kva)
			}

			var result []KeyValue
			for k, v := range kva {
				result = append(result, KeyValue{Key: k, Value: reducef(k, v)})
			}
			targetName := fmt.Sprintf("mr-out-%v", reduceIdx)
			if err := atomicWriteFile(targetName, result, false); err != nil {
				fmt.Printf("Write reduce file error: %v\n", err)
				continue
			}

			CallSubmitReduce(reduceIdx)
		}
	}

}

func CallPullMap() (string, bool) {
	args := PullMapArgs{}
	reply := PullMapReply{}

	ok := call("Coordinator.PullMap", &args, &reply)

	for !ok {
		time.Sleep(100 * time.Millisecond)
		ok = call("Coordinator.PullMap", &args, &reply)
	}

	return reply.FileName, reply.Done
}

func CallSubmitMap(fileName string, kva []KeyValue) {
	args := SubmitMapArgs{
		FileName: fileName,
	}
	reply := SubmitMapReply{}

	ok := call("Coordinator.SubmitMap", &args, &reply)

	for !ok {
		time.Sleep(100 * time.Millisecond)
		ok = call("Coordinator.SubmitMap", &args, &reply)
	}
}

func CallPullReduce() (int, bool) {
	args := PullReduceArgs{}
	reply := PullReduceReply{}

	ok := call("Coordinator.PullReduce", &args, &reply)
	for !ok {
		time.Sleep(100 * time.Millisecond)
		ok = call("Coordinator.PullReduce", &args, &reply)
	}

	return reply.ReduceIdx, reply.Done
}

func CallSubmitReduce(reduceIdx int) {
	args := SubmitReduceArgs{
		ReduceIdx: reduceIdx,
	}
	reply := SubmitReduceReply{}

	ok := call("Coordinator.SubmitReduce", &args, &reply)
	for !ok {
		time.Sleep(100 * time.Millisecond)
		ok = call("Coordinator.SubmitReduce", &args, &reply)
	}
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
