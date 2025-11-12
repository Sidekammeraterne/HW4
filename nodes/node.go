package main

import (
	proto "ITUServer/grpc" //make connection
	"context"              //make connection - the context of the connection
	"flag"                 //command-line handling
	"io"
	"log" //logs - used to keep track of messages
	"math/rand"
	"net" //make connection to net
	"os"  //terminal input
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeServer struct {
	proto.UnimplementedRicartArgawalaServer //part of the proto - we are creating an Unimplemented server
	logFile                                 *os.File
	Node                                    *Node //pointer to the node that in the server
}

type Node struct {
	Node           proto.RicartArgawalaClient
	NodeId         int32
	Lamport        int32
	LamportRequest int32
	State          string
	Address        string
	Queue          []*proto.Client
	OtherNodes     map[string]proto.RicartArgawalaClient
	OkReceived     map[int32]bool
}

type Config struct { //configuration for command-line setup, used for id and port-handling
	ID         int32
	Port       string
	OtherPorts []string //the other clients ports comma-seperated
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
	firstLogging := configuration.ID == 1 //for logfile purposes
	setupLogging(firstLogging)
	log.Println("Configuration setup complete")

	//setup node/client part of the server
	clientAddress := "localhost" + configuration.Port
	conn, err := grpc.NewClient(clientAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Not working")
	}
	client := proto.NewRicartArgawalaClient(conn)

	n := Node{
		Node:       client,
		NodeId:     configuration.ID,
		Lamport:    0,
		State:      "RELEASED",
		Address:    clientAddress,
		OtherNodes: make(map[string]proto.RicartArgawalaClient),
		OkReceived: make(map[int32]bool),
	}
	log.Printf("Node struct initialized for node %d, address %s", n.NodeId, n.Address)

	n.setupNodes(configuration)

	//setup server part
	go func() {
		server := &NodeServer{
			Node: &n, //pointer to the node object in the server
		}
		server.startServer(configuration.Port)
	}()

	//wait for the server to start
	time.Sleep(60 * time.Second)

	//configure the nodes reference to the other nodes in the system
	//n.setupNodes(configuration)
	//starts this node
	n.nodeBehavior()

}

func (n *Node) setupNodes(configuration Config) {
	//creates a client on the server for each of the other nodes in the system
	for i := 0; i < len(configuration.OtherPorts); i++ {
		clientAddress := "localhost" + configuration.OtherPorts[i]
		conn, err := grpc.NewClient(clientAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Not working")
			return
		}
		client := proto.NewRicartArgawalaClient(conn)
		//collects these clients in a map
		n.OtherNodes[clientAddress] = client
		log.Println("ADDING TO OTHER NODES", clientAddress, client)
	}
	log.Printf("Completed list of other nodes, total: %v", len(n.OtherNodes))
}

func (n *Node) nodeBehavior() {
	for {
		choice := rand.Intn(10) //returns a random number between 0 and 9
		if choice < 7 {         //if the number is less than 7 (70% chance), it will enter the critical section
			n.sendRequests()
			n.EnterCriticalSection()
			n.ExitCriticalSection()
		} else { //if the number is 7 or higher (30% chance), it will simply sleep
			time.Sleep(10 * time.Second)
		}
	}
}

func (n *Node) getClientInfo() *proto.Client {
	n.LamportRequest = n.Lamport
	return &proto.Client{
		Id:           n.NodeId,
		LamportClock: n.LamportRequest,
		Address:      n.Address,
	}
}

func (s *NodeServer) startServer(port string) {
	grpcServer := grpc.NewServer()                    //creates new gRPC server instance
	proto.RegisterRicartArgawalaServer(grpcServer, s) //registers the server implementation with gRPC

	listener, err := net.Listen("tcp", port) //listens on TCP port using net.Listen
	if err != nil {
		log.Fatalf("Did not work, failed listening on port %s", port)
	}

	log.Printf("[Server] listening on port %s", port)
	err = grpcServer.Serve(listener)
}

// utility method that compares the received lamport clock with the local one and updates it if the received one is higher
func (n *Node) updateLamportOnReceive(remoteLamport int32) int32 {
	if remoteLamport > n.Lamport {
		log.Printf("As my Lamport (%v) is lower than theirs (%v), I set my Lamport to (%v)", n.Lamport, remoteLamport, remoteLamport)
		n.Lamport = remoteLamport
	}
	n.incrementLamportClock()
	return n.Lamport
}

// utility method that increments the local lamport clock
func (n *Node) incrementLamportClock() {
	n.Lamport++
}

// sets up logging both into a file and the terminal
func setupLogging(firstTime bool) {
	var logType int
	if firstTime {
		logType = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	} else {
		logType = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	//creates the file (or overwrites it if it already exists)
	logFile, err := os.OpenFile("logFile.log", logType, 0666)
	if err != nil {
		log.Fatalf("failed to create log file: %v", err)
	}

	log.SetOutput(io.MultiWriter(os.Stdout, logFile)) //sets the output to both the terminal and the file
	log.SetFlags(log.Ldate | log.Ltime)               //metadata written before each log message
}

// sendRequests sends a request to all the other nodes in the system, requesting access to the critical section
func (n *Node) sendRequests() {
	n.State = "WANTED"
	log.Println("State change: WANTED")
	n.incrementLamportClock() //as it is about to send the request
	n.LamportRequest = n.Lamport

	//Sends enter() request to all the other nodes in the system
	for _, client := range n.OtherNodes {
		go func(client proto.RicartArgawalaClient) {
			log.Printf("Sending request EnterRequest to %v with the Lamport Timestamp %v", client, n.LamportRequest)
			_, err := client.EnterRequest(context.Background(), n.getClientInfo())
			if err != nil {
				log.Printf("Failed to send reuquest %v", err)
				return
			}
		}(client)
	}

	n.WaitForOkay() //wait for ok from all other nodes. When continuing, it means that it is allowed to enter the Critical Section
}

// EnterRequest simulates behavior of node receiving an EnterRequest() by either putting the requestee in a queue or replying okay.
func (s *NodeServer) EnterRequest(ctx context.Context, in *proto.Client) (*proto.Empty, error) {
	log.Printf("Received request from %v with Lamport timestamp %d", in.Id, in.LamportClock)
	s.Node.updateLamportOnReceive(in.LamportClock) //updates the local lamport clock on receiving the reply and increments it

	switch {
	case s.Node.State == "HELD":
		s.Node.Queue = append(s.Node.Queue, in) //puts the requestee in the queue to reply after exiting the critical section itself
		log.Printf("Added request from %v to queue as my state is HELD", in.Id)
	case s.Node.State == "WANTED" && s.Node.LamportRequest < in.LamportClock: // || s.Node.LamportRequest == in.LamportClock && s.Node.NodeId < in.Id):
		s.Node.Queue = append(s.Node.Queue, in) //puts the requestee in the queue to reply after exiting the critical section itself
		log.Printf("Added request from %v to queue as my Lamport (%v) is lower than theirs(%v)", in.Id, s.Node.LamportRequest, in.LamportClock)
	case s.Node.State == "WANTED" && s.Node.LamportRequest == in.LamportClock && s.Node.NodeId < in.Id:
		s.Node.Queue = append(s.Node.Queue, in)
		log.Printf("Added request from %v to queue as my Lamport (%v) is equal than theirs(%v), but my ID (%v) is lower than theirs (%v)", in.Id, s.Node.LamportRequest, in.LamportClock, s.Node.NodeId, in.Id)
	default:
		s.Node.incrementLamportClock() //as it is about to reply
		reply := &proto.ReplyOk{
			NodeID:       s.Node.NodeId,
			LamportClock: s.Node.Lamport,
		}
		var replyclient = s.Node.OtherNodes[in.Address]
		_, err := replyclient.ReplyOkay(context.Background(), reply)
		log.Printf("Sent ReplyOkay to %v as I am no eligible", in.Id)
		if err != nil {
			log.Printf("Failed to reply okay %v", err)
			return &proto.Empty{}, err
		}
	}
	return &proto.Empty{}, nil
}

// EnterCriticalSection simulates the node entering the critical section with a state change and a log print.
func (n *Node) EnterCriticalSection() {
	n.incrementLamportClock() //as it is about to enter the critical section
	n.State = "HELD"
	log.Println("State change: HELD")
	log.Printf("Node %d is entering the Critical Section with the Lamport timestamp (%v) \n", n.NodeId, n.Lamport)
	time.Sleep(5 * time.Second)
}

// ExitCriticalSection simulates a node exiting the critical section with a state change and a log print.
// It also sends a reply okay to all the nodes in the queue, then clears the queue.
func (n *Node) ExitCriticalSection() {
	n.incrementLamportClock() //as it is about to exit the critical section
	log.Printf("Node %d is exiting the Critical Section with the Lamport timestamp (%v)\n", n.NodeId, n.Lamport)
	n.State = "RELEASED"
	log.Println("State change: RELEASED")
	n.OkReceived = make(map[int32]bool)
	for _, queued := range n.Queue {
		n.incrementLamportClock() //as it is about to reply
		client := n.OtherNodes[queued.Address]
		_, err := client.ReplyOkay(context.Background(), &proto.ReplyOk{NodeID: n.NodeId, LamportClock: n.Lamport})
		if err != nil {
			log.Printf("Failed to reply ok from queue %v", err)
			return
		}
		log.Printf("Sent ReplyOkay to %v as with my Lamport timestamp being %v, as I just exited the Critical Section\n", queued.Id, n.Lamport)
	}
	n.Queue = nil //clears the queue as it should be empty after replying to all nodes on it.
}

// ReplyOkay handles receiving a reply okay from another node by updating the lamport clock and marking the node as having replied
func (s *NodeServer) ReplyOkay(ctx context.Context, in *proto.ReplyOk) (*proto.Empty, error) {
	log.Printf("received OK reply from %v with their Lamport timestamp being %v", in.NodeID, in.LamportClock)
	s.Node.updateLamportOnReceive(in.LamportClock) //updates the local lamport clock on receiving the reply and increments it
	s.Node.OkReceived[in.NodeID] = true
	log.Printf("Received ok: %v, Need ok in total: %v", len(s.Node.OkReceived), len(s.Node.OtherNodes))
	return &proto.Empty{}, nil
}

// WaitForOkay waits for all nodes to reply okay, then returns
func (n *Node) WaitForOkay() {
	for {
		if len(n.OkReceived) == len(n.OtherNodes) { //if all nodes have replied, the OkReceived map will have the same length as the OtherNodes map
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
