package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	MapDistributionRpcName    = "Coordinator.MapDistribution"
	MapFinishRpcName          = "Coordinator.MapFinish"
	ReduceDistributionRpcName = "Coordinator.ReduceDistribution"
	IsMapWorkDoneRpcName      = "Coordinator.IsMapWorkDone"
	IsReduceDoneRpcName       = "Coordinator.IsReduceWorkDone"
	ReduceWorkDone            = "Coordinator.ReduceWorkDone"
	HandleErrorRpcName        = "Coordinator.HandleError"
	PingRpcName               = "Coordinator.Ping"

	outputFilename = "mr-out"
)

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	// log.Println("start a worker")

	var isDone bool

	var tranChan = make(chan bool, 1)
	t := time.NewTicker(time.Second / 500)
	for {
		select {
		case <-tranChan:
			if !DealReduce(reducef) {
				// 没有分配到 reduce 操作
				var Done DoneReply
				call(IsReduceDoneRpcName, &NullReply{}, &Done)
				if Done.IsDone {
					// 全部处理完了，程序应该退出
					return
				}
			}
		case <-t.C:
			if !isDone {
				DealMap(mapf)
			}
		default:
			// 是否 map 已经处理完了
			if !isDone {
				var isMapDone DoneReply
				call(IsMapWorkDoneRpcName, &NullReply{}, &isMapDone)
				isDone = isMapDone.IsDone
			}
			if isDone {
				// 处理完了只需要进行 reduce 操作
				tranChan <- true
			}
		}
	}
}

func DealReduce(reducef func(string, []string) string) bool {
	// 当前 worker 是否可以进行 reduce 的处理
	var disReply DisReply
	ok := call(ReduceDistributionRpcName, &NullArgs{}, &disReply)
	if !ok || disReply.WorkerSeq == -1 {
		// 没拿到
		return false
	}

	// worker 的基本参数配置
	seq := disReply.WorkerSeq
	// log.Println("start reduce work, reply:", disReply.WorkerSeq)

	ctx, cancel := context.WithCancel(context.Background())
	go KeepAlive(ctx, ReduceWorker, seq)

	// 找到对应分区下的所有中间输出文件
	pattern := fmt.Sprintf("mr-*-%d.txt", seq)

	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Printf("glob file, err: %s", err.Error())
		cancel()
		call(HandleErrorRpcName, &PingArgs{
			WorkerType: seq,
			WorkerSeq:  ReduceWorker,
		}, &NullReply{})
		return true
	}

	// reduce 处理核心逻辑
	var intermediate []KeyValue
	for _, middleFilename := range files {
		// 该分区的所有文件，全是 KeyValue 的 JSON 存储
		middleFile, err := os.OpenFile(middleFilename, os.O_RDONLY, os.ModePerm)
		if err != nil {
			log.Printf("open: %s, err: %s", middleFilename, err.Error())
			cancel()
			call(HandleErrorRpcName, &PingArgs{
				WorkerType: seq,
				WorkerSeq:  ReduceWorker,
			}, &NullReply{})
			return true
		}

		content, err := io.ReadAll(middleFile)
		if err != nil {
			log.Printf("read: %s, err: %s", middleFilename, err.Error())
			cancel()
			call(HandleErrorRpcName, &PingArgs{
				WorkerType: seq,
				WorkerSeq:  ReduceWorker,
			}, &NullReply{})
			return true
		}

		var i []KeyValue
		err = json.Unmarshal(content, &i)
		if err != nil {
			log.Printf("read: %s, err: %s", middleFilename, err.Error())
			cancel()
			call(HandleErrorRpcName, &PingArgs{
				WorkerType: seq,
				WorkerSeq:  ReduceWorker,
			}, &NullReply{})
			return true
		}

		intermediate = append(intermediate, i...)
	}

	sort.Sort(ByKey(intermediate))

	output := fmt.Sprintf("%s-%d", outputFilename, seq)
	os.Remove(output)
	file, err := os.OpenFile(output, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Printf("open: %s, err: %s", output, err.Error())
		cancel()
		call(HandleErrorRpcName, &PingArgs{
			WorkerType: seq,
			WorkerSeq:  ReduceWorker,
		}, &NullReply{})
		return true
	}

	var outputRes string
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
		outputStr := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		outputRes += fmt.Sprintf("%v %v\n", intermediate[i].Key, outputStr)

		i = j
	}

	_, err = file.Write([]byte(outputRes))
	if err != nil {
		log.Printf("write: %s, err: %s", output, err.Error())
		cancel()
		call(HandleErrorRpcName, &PingArgs{
			WorkerType: seq,
			WorkerSeq:  ReduceWorker,
		}, &NullReply{})
		return true
	}

	file.Close()

	call(ReduceWorkDone, &PingArgs{WorkerType: ReduceWorker, WorkerSeq: seq}, &NullReply{})
	cancel()
	return true
}

func DealMap(mapf func(string, string) []KeyValue) {
	var disReply DisReply
	ok := call(MapDistributionRpcName, &NullArgs{}, &disReply)
	if !ok || disReply.WorkerSeq == -1 || disReply.Filename == "" {
		// 没拿到资源，那么就等待
		return
	}

	nReduce := disReply.NReduce
	seq, filename, content := disReply.WorkerSeq, disReply.Filename, disReply.Content

	var ctx, cancel = context.WithCancel(context.Background())
	go KeepAlive(ctx, MapWorker, seq)

	// 记录分区文件对应的 KeyValue 对
	var recordKF = make(map[string][]KeyValue)
	kvs := mapf(filename, content)
	for _, KV := range kvs {
		subzone := ihash(KV.Key) % nReduce
		// filename is mr-X-Y, X -> subzone num, Y -> worker seq
		fn := fmt.Sprintf("mr-%d-%d.txt", seq, subzone)
		if _, isExist := recordKF[fn]; isExist {
			recordKF[fn] = append(recordKF[fn], KV)
		} else {
			recordKF[fn] = []KeyValue{KV}
		}
	}

	for iFilename, in := range recordKF {
		res, err := json.Marshal(&in)
		if err != nil {
			log.Printf("open: %s, err: %s", iFilename, err.Error())
			call(HandleErrorRpcName, &PingArgs{
				WorkerType: seq,
				WorkerSeq:  MapWorker,
			}, &NullReply{})
			cancel()
			return
		}

		// 只写
		os.Remove(iFilename)
		file, err := os.OpenFile(iFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		if err != nil {
			log.Printf("open: %s, err: %s", iFilename, err.Error())
			call(HandleErrorRpcName, &PingArgs{
				WorkerType: seq,
				WorkerSeq:  MapWorker,
			}, &NullReply{})
			cancel()
			return
		}

		file.Write(res)
		file.Close()
	}

	call(MapFinishRpcName, &FinishArgs{WorkerSeq: seq}, &NullReply{})
	cancel()
}

func KeepAlive(ctx context.Context, wType, workSeq int) {
	var t = time.NewTicker(time.Second)
	for {
		select {
		case <-t.C:
			// log.Printf("working, seq: %d, type: %d", workSeq, wType)
			ok := call(PingRpcName, &PingArgs{wType, workSeq}, &NullReply{})
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
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
