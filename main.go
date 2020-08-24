package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"cap/task-allocator-service/config"
	pb "cap/task-allocator-service/genproto"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

var (
	configs config.Configurations
)

const (
	// CheckHandlerStatusInterval check handlers every time interval
	CheckHandlerStatusInterval = 5 // seconds
	// InactiveHandlerThresholdTime max time the handler can remain inactive until it is purged
	InactiveHandlerThresholdTime = 15 // seconds
	// MaxHandlers max possible Handlers
	MaxHandlers = 2
)

// Handler struct
type Handler struct {
	// address
	addr              string
	// status of the handler
	status            pb.HandlerStatus_Status
	// time of task assignment
	taskTimestamp     time.Time
	// time when detected inactive
	inactiveTimestamp time.Time
	// assigned task's id
	task              string
}

// TaskAllocator struct
type TaskAllocator struct {
	mu        sync.Mutex
	// map of handler to Handler instance
	handlers  map[string]*Handler
	// map of task id to Task instance
	tasks     map[string]*Task
	// queued tasks buffered channel with buffer equal to maxhandlers
	taskQueue chan string
}

// Task struct
type Task struct {
	cid    string
	name   string
	status pb.TaskStatus_Status
	handler string
}

// SubmitTask to the Allocator
func (t *TaskAllocator) SubmitTask(ctx context.Context, in *pb.Task) (*empty.Empty, error) {
	// add the task
	t.mu.Lock()
	t.tasks[in.VideoCid] = &Task{in.VideoCid, in.VideoName, pb.TaskStatus_UNASSIGNED, ""}
	// add to the queue in the background as it is a buffered channel
	go func () {
		t.taskQueue <- in.VideoCid
	}()
	t.mu.Unlock()

	return &empty.Empty{}, nil
}

// RegisterHandler to allocate a task to
func (t *TaskAllocator) RegisterHandler(ctx context.Context, in *pb.Handler) (*empty.Empty, error) {
	// add the handler 
	t.mu.Lock()
	t.handlers[in.Addr] = &Handler{in.Addr, pb.HandlerStatus_ACTIVE, time.Time{}, time.Time{}, ""}
	t.mu.Unlock()

	return &empty.Empty{}, nil
}


// register the handler as inactive in case of error during communication
func (t *TaskAllocator) manageInactiveHandler(handlerAddr string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// handler inactive 
	if (t.handlers[handlerAddr].status == pb.HandlerStatus_INACTIVE) {
		if (t.handlers[handlerAddr].inactiveTimestamp.Sub(time.Now()) > InactiveHandlerThresholdTime) {
			// remove from map

			// remove corresponding task back to queue
		}
	} else {
		// change status to inactive
		t.handlers[handlerAddr].status = pb.HandlerStatus_INACTIVE
		t.handlers[handlerAddr].inactiveTimestamp = time.Now()
	}
}


// get the status of a task assigned to a handler
func (t *TaskAllocator) taskStatus(handlerAddr string) {
	// connect
	conn, err := grpc.Dial(handlerAddr)
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
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	// if done update the status here
	if taskStatus.Status == pb.TaskStatus_DONE {
		var task *Task = t.tasks[t.handlers[handlerAddr].task];
		task.status = pb.TaskStatus_DONE
		task.handler = ""
	}
	// TODO else if timestamp is greater than some max deallocate and assign 
	// to someone else
}


// ping a inactive handler and manage status
func (t *TaskAllocator) ping(handlerAddr string) {
	// connect
	conn, err := grpc.Dial(handlerAddr)
	if err != nil {
		log.Println(err)
		t.manageInactiveHandler(handlerAddr)
		return
	}
	defer conn.Close()

	// alive 

	// possibilities
}


// check status of handlers and the tasks assigned to them
func (t *TaskAllocator) checkStatus() chan bool {
	// ticker for the interval
	ticker := time.NewTicker(CheckHandlerStatusInterval * time.Second)
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <- ticker.C:
				t.mu.Lock()
				defer t.mu.Unlock()
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

			case <- quit: 
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
		case taskCid := <- t.taskQueue:
			t.mu.Lock()
			defer t.mu.Unlock()

			// TODO take from a queue rather than iterating
			for handlerAddr, handler := range t.handlers {
				if (handler.status == pb.HandlerStatus_ACTIVE) {
					// make call to the handler
					conn, err := grpc.Dial(handlerAddr)
					if err != nil {
						log.Println(err)
						t.manageInactiveHandler(handlerAddr)
						break;
					}
					defer conn.Close()

					// allocate task
					client := pb.NewTaskAllocationServiceClient(conn)
					_, errC := client.AllocateTask(context.Background(), &pb.Task{VideoCid: taskCid, VideoName: t.tasks[taskCid].name})
					if errC != nil {
						log.Println(errC)
						break;
					}

					// update handler variables
					handler.status = pb.HandlerStatus_WORKING
					handler.task = taskCid
					handler.taskTimestamp = time.Now()

					// update task variables
					t.tasks[taskCid].status = pb.TaskStatus_ASSIGNED
					t.tasks[taskCid].handler = handlerAddr
					break;
				}
			}

		case <- quit:
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
	log.SetFlags(log.LstdFlags | log.Lshortfile )

	// listen on port
	lis, err := net.Listen("tcp", ":" + configs.Server.Port)
	if err != nil {
		log.Fatalln(err)
	}
	grpcServer := grpc.NewServer()

	allocator := &TaskAllocator{
		handlers: make(map[string]*Handler),
		tasks: make(map[string]*Task),
		taskQueue: make(chan string, MaxHandlers),
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