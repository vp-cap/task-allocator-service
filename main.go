package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"
	"strings"

	"github.com/vp-cap/task-allocator-service/config"
	pb "github.com/vp-cap/task-allocator-service/genproto"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

var (
	configs config.Configurations
)

const (
	// CheckHandlerStatusInterval check handlers every time interval
	CheckHandlerStatusInterval = 10 * time.Second
	// InactiveHandlerThresholdTime max time the handler can remain inactive until it is purged
	InactiveHandlerThresholdTime = 20 * time.Second
	// MaxHandlers max possible Handlers
	MaxHandlers = 2
	// ConnectionTimeout time to connect else timeout
	ConnectionTimeout = 3 * time.Second
)

// Handler struct
type Handler struct {
	// address
	addr string
	// status of the handler
	status pb.HandlerStatus_Status
	// time of task assignment
	taskTimestamp time.Time
	// time when detected inactive
	inactiveTimestamp time.Time
	// assigned task's id
	task string
}

// TaskAllocator struct
type TaskAllocator struct {
	mu sync.Mutex
	// map of handler to Handler instance
	handlers map[string]*Handler
	// map of task id to Task instance
	tasks map[string]*Task
	// queued tasks buffered channel with buffer equal to maxhandlers
	taskQueue chan string
	// queue handlers that are available
	handlerQueue chan string
}

// Task struct
type Task struct {
	cid     string
	name    string
	status  pb.TaskStatus_Status
	handler string
}

// SubmitTask to the Allocator
func (t *TaskAllocator) SubmitTask(ctx context.Context, in *pb.Task) (*empty.Empty, error) {
	// add the task
	t.mu.Lock()
	t.tasks[in.VideoCid] = &Task{in.VideoCid, in.VideoName, pb.TaskStatus_UNASSIGNED, ""}
	// add to the queue in the background as it is a buffered channel
	go func() {
		t.taskQueue <- in.VideoCid
	}()
	t.mu.Unlock()
	log.Println("New Task:", in.VideoCid)
	return &empty.Empty{}, nil
}

// RegisterHandler to allocate a task to
func (t *TaskAllocator) RegisterHandler(ctx context.Context, in *pb.Handler) (*empty.Empty, error) {
	// add the handler
	t.mu.Lock()
	p, _ := peer.FromContext(ctx)
	addr := strings.Split(p.Addr.String(), ":")[0] + in.Addr

	t.handlers[addr] = &Handler{addr, pb.HandlerStatus_ACTIVE, time.Time{}, time.Time{}, ""}
	go func() {
		t.handlerQueue <- addr
	}()
	t.mu.Unlock()
	log.Println("New Handler:", addr)
	return &empty.Empty{}, nil
}

// RegisterTaskComplete to note task completion and send confirmation to save
func (t *TaskAllocator) RegisterTaskComplete(ctx context.Context, in *pb.Handler) (*pb.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	handler := t.handlers[in.Addr]
	// if not removed due to inactivity
	if handler != nil {
		log.Println("Task:", handler.task, "Complete")

		delete(t.tasks, handler.task)
		// TODO update in db
		// task := t.tasks[handler.task]
		// task.status = pb.TaskStatus_DONE
		// task.handler = ""
		handler.task = ""
		handler.taskTimestamp = time.Time{}
		handler.status = pb.HandlerStatus_ACTIVE

		return &pb.Response{StoreInDb: true}, nil
	}
	return &pb.Response{StoreInDb: false}, nil
}

// register the handler as inactive in case of error during communication
func (t *TaskAllocator) manageInactiveHandler(handlerAddr string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	log.Println("Inactive Handler:", handlerAddr)
	handler := t.handlers[handlerAddr]
	// handler inactive
	if handler.status == pb.HandlerStatus_INACTIVE {
		log.Println("Time to remove? ", time.Now().Sub(handler.inactiveTimestamp) > InactiveHandlerThresholdTime)
		if time.Now().Sub(handler.inactiveTimestamp) > InactiveHandlerThresholdTime {
			// remove corresponding task back to queue
			if handler.task != "" {
				task := t.tasks[handler.task]
				task.handler = ""
				task.status = pb.TaskStatus_UNASSIGNED
			}

			// remove from map
			delete(t.handlers, handlerAddr)
			log.Println("Inactive handler removed:", handlerAddr)
		}
	} else {
		// change status to inactive
		handler.status = pb.HandlerStatus_INACTIVE
		handler.inactiveTimestamp = time.Now()
		log.Println("Inactive handler:", handlerAddr, "inactiveTimestamp:", handler.inactiveTimestamp)
	}
}

// get the status of a task assigned to a handler
func (t *TaskAllocator) taskStatus(handlerAddr string) {
	// connect
	conn, err := grpc.Dial(handlerAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(ConnectionTimeout))
	if err != nil {
		log.Println(err)
		t.manageInactiveHandler(handlerAddr)
		return
	}

	defer conn.Close()
	// get task
	client := pb.NewTaskAllocationServiceClient(conn)
	taskStatus, errC := client.GetTaskStatus(context.Background(), &empty.Empty{})
	if errC != nil {
		log.Println(errC)
		t.manageInactiveHandler(handlerAddr)
		return
	}
	log.Println("Handler:", handlerAddr, "taskStatus:", taskStatus)

	if taskStatus.Status == pb.TaskStatus_FAILED {
		// retry with another handler??
		handler := t.handlers[handlerAddr]
		delete(t.tasks, handler.task)
		// TODO update in db
		// task := t.tasks[handler.task]
		// task.status = pb.TaskStatus_FAILED
		// task.handler = ""
		handler.task = ""
		handler.taskTimestamp = time.Time{}
		handler.status = pb.HandlerStatus_ACTIVE
	}

	// TODO else if timestamp is greater than some max deallocate and assign
	// to someone else
}

// ping a inactive handler and manage status
func (t *TaskAllocator) ping(handlerAddr string) {
	log.Println("Pinging handler:", handlerAddr)
	// connect
	conn, err := grpc.Dial(handlerAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(ConnectionTimeout))
	if err != nil {
		log.Println(err)
		t.manageInactiveHandler(handlerAddr)
		return
	}
	defer conn.Close()

	// alive
	t.mu.Lock()
	defer t.mu.Unlock()
	handler := t.handlers[handlerAddr]
	handler.inactiveTimestamp = time.Time{}

	if handler.task != "" {
		handler.status = pb.HandlerStatus_WORKING
	} else {
		handler.status = pb.HandlerStatus_ACTIVE
	}
	log.Println("Handler:", handlerAddr, "Alive! status: ", handler.status)
}

// check status of handlers and the tasks assigned to them
func (t *TaskAllocator) checkStatus() chan bool {
	// ticker for the interval
	ticker := time.NewTicker(CheckHandlerStatusInterval)
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				t.mu.Lock()
				// go through the handler list
				for handlerAddr, handler := range t.handlers {
					switch handler.status {
					// original status is working then check task status
					case pb.HandlerStatus_WORKING:
						go t.taskStatus(handlerAddr)
					case pb.HandlerStatus_INACTIVE:
						go t.ping(handlerAddr)
					}
				}
				t.mu.Unlock()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
	return quit
}

// allocate task
func (t *TaskAllocator) allocateTask(quit chan bool) {
	for {
		select {
		case taskCid := <-t.taskQueue:
			log.Println("Allocate Task:", taskCid)

			handler := t.handlers[<-t.handlerQueue]
			if handler == nil {
				t.mu.Lock()
				t.taskQueue <- taskCid
				t.mu.Unlock()
				log.Println("Handler does not exists, adding task back to queue")
				break
			}
			if handler.status == pb.HandlerStatus_ACTIVE {
				// make call to the handler
				conn, err := grpc.Dial(handler.addr, grpc.WithInsecure())
				if err != nil {
					t.mu.Lock()
					t.taskQueue <- taskCid
					t.mu.Unlock()
					log.Println(err)
					log.Println("Adding task back to queue")
					t.manageInactiveHandler(handler.addr)
					break
				}
				defer conn.Close()

				// allocate task
				client := pb.NewTaskAllocationServiceClient(conn)
				_, errC := client.AllocateTask(context.Background(), &pb.Task{VideoCid: taskCid, VideoName: t.tasks[taskCid].name})
				if errC != nil {
					t.mu.Lock()
					t.taskQueue <- taskCid
					t.mu.Unlock()
					log.Println(errC)
					log.Println("Adding task back to queue")
					break
				}

				t.mu.Lock()
				// update handler variables
				handler.status = pb.HandlerStatus_WORKING
				handler.task = taskCid
				handler.taskTimestamp = time.Now()

				// update task variables
				t.tasks[taskCid].status = pb.TaskStatus_ASSIGNED
				t.tasks[taskCid].handler = handler.addr
				t.mu.Unlock()

				log.Println("Task:", taskCid, "alloacted to handler:", handler.addr)
			}
		case <-quit:
			return
		}
	}
}

// create the task allocators
func (t *TaskAllocator) allocateTaskManager() chan bool {
	quit := make(chan bool)
	for i := 0; i < MaxHandlers; i++ {
		go t.allocateTask(quit)
	}
	log.Println("Allocation routines started")
	return quit
}

func init() {
	var err error
	configs, err = config.GetConfigs()
	if err != nil {
		log.Println("Unable to get config")
	}
}

func main() {
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// listen on port
	lis, err := net.Listen("tcp", ":"+configs.Server.Port)
	if err != nil {
		log.Fatalln(err)
	}
	grpcServer := grpc.NewServer()

	allocator := &TaskAllocator{
		handlers:     make(map[string]*Handler),
		tasks:        make(map[string]*Task),
		taskQueue:    make(chan string, MaxHandlers),
		handlerQueue: make(chan string, MaxHandlers),
	}

	pb.RegisterTaskInitServiceServer(grpcServer, allocator)
	pb.RegisterRegisterHandlerServiceServer(grpcServer, allocator)

	quitStatusCheck := allocator.checkStatus()
	quitAllocation := allocator.allocateTaskManager()

	// serve
	log.Println("Serving on", configs.Server.Port)
	grpcServer.Serve(lis)

	quitStatusCheck <- true
	quitAllocation <- true
}
