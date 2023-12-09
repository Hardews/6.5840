package mr

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

const (
	MapWorker = iota
	ReduceWorker

	MaxTimeLimit = 10
)

type Coordinator struct {
	Mu                              sync.Mutex `json:"-"`
	Exp                             time.Duration
	NReduce                         int
	IsDone, IsMapDone               bool
	InputFiles                      []string
	DistributionChan                chan FileInfo `json:"-"`
	MapWorkerNum, ReduceWorkerNum   int
	MapChan, ReduceChan             chan int `json:"-"`
	MapWorkerInfo, ReduceWorkerInfo map[int]WorkerStatus
	MapAlive, ReduceAlive           []bool
}

type FileInfo struct {
	Filename string
	Content  string
}

type WorkerStatus struct {
	Filename string // 该 worker 正在处理的文件名（仅 reduce worker
	Content  string // 该 worker 正在处理的内容（仅 map worker
}

/*	Map	*/

func (c *Coordinator) MapDistribution(args *NullArgs, reply *DisReply) error {
	reply.WorkerSeq = -1
	if len(c.DistributionChan) == 0 {
		return nil
	}

	select {
	case seq := <-c.MapChan:
		// 有空闲的 worker 位置
		reply.WorkerSeq = seq
		reply.NReduce = c.NReduce

		fileInfo := <-c.DistributionChan
		reply.Filename = fileInfo.Filename
		reply.Content = fileInfo.Content

		context.WithCancel(context.Background())
		c.Mu.Lock()
		c.MapWorkerInfo[seq] = WorkerStatus{
			Filename: fileInfo.Filename,
			Content:  fileInfo.Content,
		}
		c.MapWorkerNum++
		c.Mu.Unlock()

		c.MapAlive[seq] = true

		// log.Println("distribution a worker, seq:", seq)

		go c.handleAlive(MapWorker, seq, c.Exp)
		return nil
	default:
	}

	return nil
}

func (c *Coordinator) MapFinish(args *FinishArgs, reply *NullReply) error {
	// 删除信息
	c.MapChan <- args.WorkerSeq

	c.Mu.Lock()
	delete(c.MapWorkerInfo, args.WorkerSeq)
	c.MapWorkerNum--
	c.Mu.Unlock()

	c.MapAlive[args.WorkerSeq] = false

	if c.MapWorkerNum == 0 && len(c.DistributionChan) == 0 {
		// 所有的 map worker 都处理完了
		c.IsMapDone = true
	}

	return nil
}

func (c *Coordinator) contentDistribution() {
	for len(c.InputFiles) != 0 {
		filename := c.InputFiles[0]
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("open: %s, err: %s", filename, err.Error())
		}

		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("read: %s, err: %s", filename, err.Error())
		}

		c.DistributionChan <- FileInfo{
			Filename: filename,
			Content:  string(content),
		}

		// 出队
		c.InputFiles = c.InputFiles[1:]
	}
}

/*	Reduce	*/

func (c *Coordinator) ReduceDistribution(args *NullArgs, reply *DisReply) error {
	reply.WorkerSeq = -1
	if len(c.ReduceChan) == 0 {
		return nil
	}

	select {
	case seq := <-c.ReduceChan:
		c.ReduceAlive[seq] = true

		reply.WorkerSeq = seq

		c.Mu.Lock()
		c.ReduceWorkerNum++
		c.ReduceWorkerInfo[seq] = WorkerStatus{}
		c.Mu.Unlock()

		// log.Printf("reduce distribute, seq: %d, chan len: %d, reduce num: %d", seq, len(c.ReduceChan), c.ReduceWorkerNum)
		go c.handleAlive(ReduceWorker, seq, c.Exp)
		return nil
	default:
	}

	return nil
}

func (c *Coordinator) IsMapWorkDone(args *NullArgs, reply *DoneReply) error {
	reply.IsDone = c.IsMapDone
	return nil
}

func (c *Coordinator) IsReduceWorkDone(args *NullArgs, reply *DoneReply) error {
	reply.IsDone = c.IsDone
	return nil
}

func (c *Coordinator) ReduceWorkDone(args *PingArgs, reply *NullReply) error {
	c.Mu.Lock()
	delete(c.ReduceWorkerInfo, args.WorkerSeq)
	c.ReduceWorkerNum--
	c.Mu.Unlock()

	// log.Printf("reduce worker num: %d, map is done: %v, done seq: %d", c.ReduceWorkerNum, c.IsMapDone, args.WorkerSeq)

	pattern := fmt.Sprintf("mr-*-%d.txt", args.WorkerSeq)
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Println("delete temp file failed, err:", err)
	}

	for _, filename := range files {
		// log.Println(filename)
		err = os.Remove(filename)
		if err != nil {
			log.Printf("clean: %s, err: %s", filename, err.Error())
		}
	}

	if len(c.ReduceChan) == 0 && c.ReduceWorkerNum == 0 && c.IsMapDone {
		c.IsDone = true
	}

	return nil
}

/*	Something All Need	*/

// HandleError worker 处理时出错，主动断开
func (c *Coordinator) HandleError(args *PingArgs, reply *NullReply) error {
	c.giveWorkBack(args.WorkerType, args.WorkerSeq)

	return nil
}

// Ping 判断是否存活的依据
func (c *Coordinator) Ping(args *PingArgs, reply *NullReply) error {
	c.setAliveStatus(args.WorkerType, args.WorkerSeq, true)
	return nil
}

// 设置这个 worker 的状态
func (c *Coordinator) setAliveStatus(wType, workerSeq int, status bool) {
	switch wType {
	case MapWorker:
		c.MapAlive[workerSeq] = status
	case ReduceWorker:
		c.ReduceAlive[workerSeq] = status
	}
}

// 出错的超时的 worker 送回队列，并将其正在处理的任务送入队列
func (c *Coordinator) giveWorkBack(wType, seq int) {
	// log.Printf("give work back, wType: %d, seq: %d", wType, seq)
	switch wType {
	case MapWorker:
		c.MapChan <- seq

		c.Mu.Lock()
		wi := c.MapWorkerInfo[seq]
		delete(c.MapWorkerInfo, seq)
		c.MapWorkerNum--
		c.Mu.Unlock()

		c.DistributionChan <- FileInfo{
			Filename: wi.Filename,
			Content:  wi.Content,
		}
	case ReduceWorker:
		c.ReduceChan <- seq

		c.Mu.Lock()
		c.ReduceWorkerNum--
		c.Mu.Unlock()
	}
}

// 一个 goroutine，异步处理每一个正在工作的 worker
// 当达到了超时时间，还没调用 Ping 函数，则视为 done 掉
func (c *Coordinator) handleAlive(wType, workerSeq int, exp time.Duration) {
	t := time.NewTicker(exp / 5)
	for {
		select {
		case <-t.C:
			switch wType {
			case MapWorker:
				if _, ok := c.MapWorkerInfo[workerSeq]; !ok {
					// 这个资源已经完成了
					return
				}
				if !c.MapAlive[workerSeq] {
					// 这个 worker 寄了，释放它处理的资源
					c.giveWorkBack(wType, workerSeq)
					return
				}
			case ReduceWorker:
				if _, ok := c.ReduceWorkerInfo[workerSeq]; !ok {
					return
				}
				if !c.ReduceAlive[workerSeq] {
					c.giveWorkBack(wType, workerSeq)
					return
				}
			}
			c.setAliveStatus(wType, workerSeq, false)
		default:
		}
	}
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// log.Println("check done status")
	return c.IsDone
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

// MakeCoordinator create a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// log.Println("make coordinator")
	c := Coordinator{}

	c.NReduce = nReduce

	// 先运行内容分发程序
	c.InputFiles = files
	c.DistributionChan = make(chan FileInfo, nReduce)
	go c.contentDistribution()

	// map 处理队列
	c.MapChan = make(chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.MapChan <- i
	}

	// reduce 处理队列
	c.ReduceChan = make(chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceChan <- i
	}

	c.MapWorkerInfo = make(map[int]WorkerStatus)
	c.ReduceWorkerInfo = make(map[int]WorkerStatus)

	c.MapAlive = make([]bool, nReduce)
	c.ReduceAlive = make([]bool, nReduce)

	c.Exp = MaxTimeLimit * time.Second

	c.server()
	return &c
}
