package main

import (
	"context"
	"log"
	"net"

	"cap/task-allocator-service/config"
	pb "cap/task-allocator-service/genproto"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

var (
	configs config.Configurations
)

// Handler struct
type Handler struct {
	Addr   string
	Status pb.HandlerStatus_Status
}

// TaskAllocator struct
type TaskAllocator struct {
	handlers  map[string]Handler
	tasks     map[string]TaskUnit
	taskQueue chan string
}

// TaskUnit struct
type TaskUnit struct {
	Cid    string
	Name   string
	Status pb.TaskStatus_Status
	Worker string
}

// SubmitTask to the Allocator
func (t *TaskAllocator) SubmitTask(ctx context.Context, in *pb.Task) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

// RegisterHandler to allocate a task to
func (t *TaskAllocator) RegisterHandler(ctx context.Context, in *pb.Handler) (*empty.Empty, error) {
	return &empty.Empty{}, nil
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

	}

	pb.RegisterTaskInitServiceServer(grpcServer, allocator)
	pb.RegisterRegisterHandlerServiceServer(grpcServer, allocator)

	// serve
	log.Println("Serving on", configs.Server.Port)
	grpcServer.Serve(lis)
}