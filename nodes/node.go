package nodes

import (
	proto "ITUServer/grpc" //make connection
	"bufio"
	"context" //make connection - the context of the connection
	"errors"  //create custom errors
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log" //logs - used to keep track of messages
	"net" //make connection to net
	"os"
	"unicode/utf8" //used to verify number of chars
)

type NodeServer struct {
	proto.UnimplementedNodeServer //part of the proto - we are creating an Unimplemented server
	lamportClock                  int32
	logFile                       *os.File
}

type Node struct {
	NodeId int32
	State  string
}

func main() {
	server := &NodeServer{}
	//creation of clients here
	server.start_server()
}

func (s *NodeServer) start_server() {
	//local event: server start
	s.lamportClock++

	grpcServer := grpc.NewServer()          //creates new gRPC server instance
	proto.RegisterNodeServer(grpcServer, s) //registers the server implementation with gRPC

	listener, err := net.Listen("tcp", ":5050") //listens on TCP port using net.Listen
	if err != nil {
		log.Fatalf("Did not work")
	}

	if err != nil {
		log.Fatalf("Did not work")
	}
}

// utility method that on message receive checks max lamport and updates local
func (s *NodeServer) updateLamportClockOnReceive(remoteLamport int32) int32 {
	if remoteLamport > s.lamportClock {
		s.lamportClock = remoteLamport
	}
	s.lamportClock++
	return s.lamportClock
}

// sets up logging both into a file and the terminal
func (s *NodeServer) setupLogging() {
	//creates the file (or overwrites it if it already exists)
	logFile, err := os.OpenFile("logFile.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("failed to create log file: %v", err)
	}

	s.logFile = logFile //saves the logfile to the client struct

	log.SetOutput(io.MultiWriter(os.Stdout, logFile)) //sets the output to both the terminal and the file
}
