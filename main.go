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
	// CheckHandlerStatusInterval period
	CheckHandlerStatusInterval = 5 // seconds
	
)

// Handler struct
type Handler struct {
	addr      string
	status    pb.HandlerStatus_Status
	timestamp time.Time
	task      string
}

// TaskAllocator struct
type TaskAllocator struct {
	mu        sync.Mutex
	handlers  map[string]*Handler
	tasks     map[string]*TaskUnit
	taskQueue chan string
}

// TaskUnit struct
type TaskUnit struct {
	cid    string
	// Name   string
	status pb.TaskStatus_Status
	handler string
}

// SubmitTask to the Allocator
func (t *TaskAllocator) SubmitTask(ctx context.Context, in *pb.Task) (*empty.Empty, error) {
	
	// add the task
	t.mu.Lock()
	t.tasks[in.VideoCid] = &TaskUnit{in.VideoCid, pb.TaskStatus_UNASSIGNED, ""}

	t.mu.Unlock()

	return &empty.Empty{}, nil
}

// RegisterHandler to allocate a task to
func (t *TaskAllocator) RegisterHandler(ctx context.Context, in *pb.Handler) (*empty.Empty, error) {
	// add the handler 
	t.mu.Lock()
	t.handlers[in.Addr] = &Handler{in.Addr, pb.HandlerStatus_ACTIVE, time.Time{}, ""}
	t.mu.Unlock()

	return &empty.Empty{}, nil
}


// get the status of a task assigned to a handler
func (t *TaskAllocator) taskStatus(handlerAddr string) {
	// connect
	conn, err := grpc.Dial(handlerAddr)
	if err != nil {
		log.Println(err)
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
		// if done update the status here
		if taskStatus.Status == pb.TaskStatus_DONE {
			var taskUnit *TaskUnit = t.tasks[t.handlers[handlerAddr].task];
			taskUnit.status = pb.TaskStatus_DONE
			taskUnit.handler = ""
		}
	t.mu.Unlock()
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
						// go ping(handlerAddr)
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
		tasks: make(map[string]*TaskUnit),
		taskQueue: make(chan string),
	}

	pb.RegisterTaskInitServiceServer(grpcServer, allocator)
	pb.RegisterRegisterHandlerServiceServer(grpcServer, allocator)

	quit := allocator.checkStatus()

	// serve
	log.Println("Serving on", configs.Server.Port)
	grpcServer.Serve(lis)

	quit <- true
}