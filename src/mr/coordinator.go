package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	MapJob = iota
	ReduceJob

	MaxTimeLimit = 10
)

type Coordinator struct {
	// base
	Mu         sync.Mutex
	Exp        time.Duration
	NReduce    int
	InputFiles []string

	// Distribute use
	DistributeSeqChan chan int
	DistributeChan    chan JobInfo

	// Job info
	JobAlive                                  []chan bool
	JobInfo                                   map[int]JobInfo
	IsAllJobDone, IsMapJobDone, IsReduceReady bool
	MapWorkerNum, ReduceWorkerNum             int
}

type JobInfo struct {
	Filename string // 该 worker 正在处理的文件名（仅 reduce worker
	Content  string // 该 worker 正在处理的内容（仅 map worker
}

/*	Job Distribute	*/

func (c *Coordinator) JobDistribute(args *DisArgs, reply *DisReply) error {
	reply.JobSeq = -1
	if len(c.DistributeSeqChan) == 0 || c.IsAllJobDone {
		return nil
	}

	if args.JobType == MapJob && c.IsMapJobDone {
		return nil
	}

	select {
	case seq := <-c.DistributeSeqChan:
		if !c.IsMapJobDone {
			// map job
			jobInfo, ok := <-c.DistributeChan
			if !ok {
				// 已经没任务了
				c.DistributeSeqChan <- seq
				return nil
			}

			reply.Filename = jobInfo.Filename
			reply.Content = jobInfo.Content

			c.Mu.Lock()
			c.JobInfo[seq] = jobInfo
			c.MapWorkerNum++
			c.Mu.Unlock()

			// log.Printf("distribute a map job, seq: %d", seq)
		} else if c.IsReduceReady {
			// reduce job
			c.Mu.Lock()
			c.ReduceWorkerNum++
			c.JobInfo[seq] = JobInfo{}
			c.Mu.Unlock()

			// log.Printf("distribute a reduce job, seq: %d", seq)
		} else {
			c.DistributeSeqChan <- seq
			return nil
		}

		// 通用信息
		reply.JobSeq = seq
		reply.NReduce = c.NReduce
		go c.handleAlive(seq, c.Exp)
	default:
	}

	return nil
}

/*	Map	*/

func (c *Coordinator) MapJobFinish(args *SeqArgs, reply *NullReply) error {
	// 删除信息
	c.Mu.Lock()
	delete(c.JobInfo, args.JobSeq)
	c.MapWorkerNum--
	c.Mu.Unlock()

	c.DistributeSeqChan <- args.JobSeq

	// log.Printf("map job finish, seq: %d, now num: %d, content chan: %d", args.JobSeq, c.MapWorkerNum, len(c.DistributeChan))

	if !c.IsMapJobDone && c.MapWorkerNum == 0 && len(c.DistributeChan) == 0 && len(c.InputFiles) == 0 {
		// 所有的 map worker 都处理完了
		c.reduceJobStart()
		c.IsMapJobDone = true
	}

	return nil
}

func (c *Coordinator) contentDistribute() {
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

		c.DistributeChan <- JobInfo{
			Filename: filename,
			Content:  string(content),
		}

		// 出队
		c.InputFiles = c.InputFiles[1:]
	}
}

func (c *Coordinator) reduceJobStart() {
	log.Println("start reduce job")
	c.DistributeSeqChan = make(chan int, c.NReduce)
	for i := 0; i < c.NReduce; i++ {
		c.DistributeSeqChan <- i
	}
	c.JobInfo = make(map[int]JobInfo)
	c.JobAlive = make([]chan bool, c.NReduce)
	c.IsReduceReady = true
}

/*	Reduce	*/

func (c *Coordinator) ReduceJobFinish(args *SeqArgs, reply *NullReply) error {
	c.Mu.Lock()
	delete(c.JobInfo, args.JobSeq)
	c.ReduceWorkerNum--
	c.Mu.Unlock()

	// log.Printf("finish reduce job, seq: %d, seq distribute: %d, reduce worker num: %d", args.JobSeq, len(c.DistributeSeqChan), c.ReduceWorkerNum)

	pattern := fmt.Sprintf("mr-*-%d.txt", args.JobSeq)
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

	if len(c.DistributeSeqChan) == 0 && c.ReduceWorkerNum == 0 && c.IsMapJobDone {
		time.Sleep(c.Exp)
		if len(c.DistributeSeqChan) == 0 && c.ReduceWorkerNum == 0 && c.IsMapJobDone {
			c.IsAllJobDone = true
		}
	}

	return nil
}

/*	Something All Need	*/

func (c *Coordinator) IsMapDone(args *NullArgs, reply *DoneReply) error {
	reply.IsDone = c.IsMapJobDone
	return nil
}

func (c *Coordinator) IsReduceDone(args *NullArgs, reply *DoneReply) error {
	reply.IsDone = c.IsAllJobDone
	return nil
}

// HandleError worker 处理时出错，主动断开
func (c *Coordinator) HandleError(args *SeqArgs, reply *NullReply) error {
	log.Println("handle error, seq: ", args.JobSeq)
	c.giveJobBack(args.JobSeq)
	return nil
}

// Ping 判断是否存活的依据
func (c *Coordinator) Ping(args *SeqArgs, reply *NullReply) error {
	go func() {
		c.JobAlive[args.JobSeq] <- true
	}()
	return nil
}

// 一个 goroutine，异步处理每一个正在工作的 worker
// 当达到了超时时间，还没调用 Ping 函数，则视为 done 掉
func (c *Coordinator) handleAlive(workerSeq int, exp time.Duration) {
	c.JobAlive[workerSeq] = make(chan bool, 1)
	c.JobAlive[workerSeq] <- true

	t := time.NewTicker(exp)
	for {
		select {
		case <-c.JobAlive[workerSeq]:
			// log.Printf("Get Ping, seq: %d", workerSeq)
			if _, ok := c.JobInfo[workerSeq]; !ok {
				// 这个资源已经完成了
				return
			}
			t.Reset(exp)
		case <-t.C:
			if _, ok := c.JobInfo[workerSeq]; !ok {
				// 这个资源已经完成了
				return
			}
			// 这个 worker 寄了，释放它处理的资源
			log.Printf("job seq: %d, time limit done", workerSeq)
			c.giveJobBack(workerSeq)
			return
		default:
		}
	}
}

// 出错的超时的 worker 送回队列，并将其正在处理的任务送入队列
func (c *Coordinator) giveJobBack(seq int) {
	if !c.IsMapJobDone && !c.IsReduceReady && !c.IsAllJobDone {
		pattern := fmt.Sprintf("mr-%d-*.txt", seq)
		files, err := filepath.Glob(pattern)
		if err != nil {
			log.Printf("glob: %v, err: %v", pattern, err)
		}
		for _, filename := range files {
			os.Remove(filename)
		}

		c.Mu.Lock()
		wi := c.JobInfo[seq]
		delete(c.JobInfo, seq)
		c.MapWorkerNum--
		c.Mu.Unlock()

		c.DistributeChan <- JobInfo{
			Filename: wi.Filename,
			Content:  wi.Content,
		}
		// log.Printf("give map job back, seq have: %d, back seq: %d, now job num: %d, content: %d", len(c.DistributeChan), seq, c.MapWorkerNum, len(c.DistributeChan))

		c.DistributeSeqChan <- seq

		return
	}

	c.Mu.Lock()
	c.ReduceWorkerNum--
	delete(c.JobInfo, seq)
	c.Mu.Unlock()
	c.DistributeSeqChan <- seq
	// log.Printf("give reduce job back, seq have: %d, back seq: %d, now job num: %d", len(c.DistributeSeqChan), seq, c.ReduceWorkerNum)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
func (c *Coordinator) Done() bool {
	return c.IsAllJobDone
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
	c := Coordinator{}
	c.NReduce = nReduce

	// 先运行内容分发程序
	c.InputFiles = files
	c.DistributeChan = make(chan JobInfo, nReduce)
	go c.contentDistribute()

	// seq 分发队列
	c.DistributeSeqChan = make(chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.DistributeSeqChan <- i
	}

	c.JobInfo = make(map[int]JobInfo)
	c.JobAlive = make([]chan bool, c.NReduce)

	c.Exp = MaxTimeLimit * time.Second

	c.server()
	return &c
}
