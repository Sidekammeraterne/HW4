package main

import (
	proto "ITUServer/grpc" //make connection
	"context" //make connection - the context of the connection
	"flag"    //command-line handling
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"io"
	"log" //logs - used to keep track of messages
	"net" //make connection to net
	"os"
)

type NodeServer struct {
	proto.UnimplementedRicartArgawalaServer //part of the proto - we are creating an Unimplemented server
	logFile                                 *os.File
}

type Node struct {
	Node           proto.RicartArgawalaClient
	NodeId         int32
	Lamport        int32
	LamportRequest int32
	State          string
	Queue          []*proto.Client
	OtherNodes   map[int32]proto.RicartArgawalaClient
}

type Config struct { //configuration for command-line setup, used for id and port-handling
	ID         int32
	Port       string
	OtherPorts []string //the other clients ports comma-seperated //todo: rename?
}

// flag for setting up a new id, ports etc. Part of Config struct
func configurationSetup() Config {
	id := flag.Int("ID", 1, "node ID")
	port := flag.String("Port", ":5050", "listen address, e.g. :5050")
	otherPorts := flag.String("OtherPorts", "", "comma-seperated hosts, made of port of others")
	flag.Parse()

	var otherPortsHosts []string
	if *otherPorts != "" {
		otherPortsHosts = strings.Split(*otherPorts, ",")
	}
	return Config{
		ID:         int32(*id),
		Port:       *port,
		OtherPorts: otherPortsHosts}
}

func main() {
	configuration := configurationSetup()
	//setup server part
	server := &NodeServer{}
	server.start_server(configuration.Port)
	log.Printf("DEBUG - ID=%d Port=%s", configuration.ID, configuration.Port) //todo delete debug

	//setup the client part //todo: do for each port in 'OtherPorts' in the config
	clientAddress := "localhost" + configuration.Port
	conn, err := grpc.NewClient(clientAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Not working")
	}

	client := proto.NewRicartArgawalaClient(conn)
	//todo: save 'client' in a list with clients for 'OtherPorts'

	n := Node{
		Node: client,
		NodeId: configuration.ID//TODO: set node id, how? given in the commandline? yes - doneee
		Lamport: 0,
		State:   "RELEASED",
	}

	go start_client(n)

}

func start_client(n Node) {
	if n.State == "RELEASED" {
		n.Lamport++
		n.State = "WANTED"
		//todo: call EnterRequest for all other clients (configured from 'OtherPorts')
		//todo: spawn go routines for each request and wait for answer
		response, err := n.Node.EnterRequest(context.Background(), n.getClientInfo())
		if err != nil {
			log.Fatalf("did not recieve anything or failed to send %v", err)
		}
		//go routine der lytter svar på alle
	}
}

func (n *Node) getClientInfo() *proto.Client {
	n.LamportRequest = n.Lamport
	return &proto.Client{
		Id:           n.NodeId,
		LamportClock: n.Lamport,
	}
}

func (s *NodeServer) start_server(port string) {
	grpcServer := grpc.NewServer()                    //creates new gRPC server instance
	proto.RegisterRicartArgawalaServer(grpcServer, s) //registers the server implementation with gRPC

	listener, err := net.Listen("tcp", port) //listens on TCP port using net.Listen
	if err != nil {
		log.Fatalf("Did not work, failed listening on port %s", port)
	}

	if err != nil {
		log.Fatalf("Did not work")
	}

	err = grpcServer.Serve(listener)
	log.Printf("[Server] listening on port", port)
}

// utility method that on message receive checks max lamport and updates local
func (s *Node) updateLamportClockOnReceive(remoteLamport int32) int32 {
	if remoteLamport > s.Lamport {
		s.Lamport = remoteLamport
	}
	s.Lamport++
	return s.Lamport
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
	log.SetFlags(log.Ldate | log.Ltime) //metadata written before each log message todo: include
}

/*
	function for when a node recieves a request to enter the critical section from another node

If this node has the state "HELD", it puts the other node in a queue
If this node has the state "WANTED"
*/
func (s *Node) EnterRequest(ctx context.Context, in *proto.Client) (*proto.Reply, error) {

	if s.State == "HELD" || s.State == "WANTED" && s.Lamport < in.LamportClock {
		s.Queue = append(s.Queue, in)

		//go routine til når den så selv har exittet
		go s.WaitForReleased() //slet goroutine.
		//return Empty
	}

	return &proto.Reply{Okay: "ok"}, nil
}

// go function, keeps looks at the state until it is set to released. Then returns, which "breaks" the go runction"
func (s *Node) WaitForReleased() {
	if s.State == "RELEASED" {
		return
	}
}

// sets the state to released, which then triggers the go routine WaitForReleased to end, such that the node will reply to all requests in the queue
func (s *Node) Exit() {
	s.State = "RELEASED"
}

//Make the queue: var requestQueue []string
//Queue: requestQueue = append(requestQueue, request)
//Dequeue: requestQueue = requestQueue[1:] //dequeue
