package main

import (
	proto "ITUServer/grpc" //make connection
	"context"              //make connection - the context of the connection
	"flag"                 //command-line handling
	"io"
	"log" //logs - used to keep track of messages
	"math/rand"
	"net" //make connection to net
	"os"
	"strings"
	"sync"
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
	log.Println("Configuration setup complete")

	//setup node/client part of the server
	clientAddress := "localhost" + configuration.Port
	conn, err := grpc.NewClient(clientAddress, grpc.WithTransportCredentials(insecure.NewCredentials())) //todo: jeg er måske dum men skal den lave connection til sig selv?
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
	log.Printf("Node struct initialized")

	//setup server part //todo: only done in go function because start_server is blocking and needed to insert wait time afterwards
	go func() {
		server := &NodeServer{
			Node: &n, //pointer to the node object in the server
		}
		server.startServer(configuration.Port)
		log.Printf("DEBUG - ID=%d Port=%s", configuration.ID, configuration.Port) //todo delete debug
	}()

	//wait for the server to start //todo: this does not work - if you are really fast a opening the different terminals it might work
	log.Println("Started sleeping")
	time.Sleep(120 * time.Second) //todo: delete only inserted for debugging
	log.Println("Stopped sleeping")

	//configure the nodes reference to the other nodes in the system
	n.setupNodes(configuration)
	//starts this node
	go n.nodeBehavior()
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
	log.Printf("Completed list of other nodes")
}

func (n *Node) nodeBehavior() {
	//todo: the todo below was put next to an if statement before, which has been put into choice < 7. I think it is still relevant but I am not sure.
	//Todo: this introduces a race condition has all the nodes will immediately request access to the Critical Section
	choice := rand.Intn(10) //returns a random number between 0 and 9
	if choice < 7 {         //if the number is less than 7 (70% chance), it will enter the critical section
		n.State = "WANTED"

		//The WaitGroup keeps track of how many go routines are running
		wg := new(sync.WaitGroup)

		n.incrementLamportClock() //increments the local lamport clock as it is about to send the request
		//Sends enter() request to all the other nodes in the system
		for _, other := range n.OtherNodes {
			wg.Add(1)
			go n.sendRequest(other, wg)
			log.Println("SPAWNED GO ROUTINE FOR REQUEST", other)
		}

		wg.Wait() //waits for the go routines to terminate, will continue when all requests have been received

		n.WaitForOkay() //waits for ok from all other nodes. When continuing, it means that it is allowed to enter the Critical Section

		n.EnterCriticalSection()
		n.ExitCriticalSection()

	} else { //if the number is 7 or higher (30% chance), it will simply sleep
		time.Sleep(10 * time.Second) //todo: can be changed if we want something else.
	}

}

func (n *Node) getClientInfo() *proto.Client {
	n.LamportRequest = n.Lamport
	return &proto.Client{
		Id:           n.NodeId,
		LamportClock: n.Lamport,
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
		n.Lamport = remoteLamport
	}
	n.incrementLamportClock()
	return n.Lamport
}

// utility method that increments the local lamport clock
func (n *Node) incrementLamportClock() {
	n.Lamport++
}

// sets up logging both into a file and the terminal //todo: implement
func (s *NodeServer) setupLogging() {
	//creates the file (or overwrites it if it already exists)
	logFile, err := os.OpenFile("logFile.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("failed to create log file: %v", err)
	}

	s.logFile = logFile //saves the logfile to the client struct

	log.SetOutput(io.MultiWriter(os.Stdout, logFile)) //sets the output to both the terminal and the file
	log.SetFlags(log.Ldate | log.Ltime)               //metadata written before each log message todo: include
}

// Makes rpc call with enter() request to the given node
func (n *Node) sendRequest(client proto.RicartArgawalaClient, wg *sync.WaitGroup) {
	log.Printf("Sending request EnterRequest to %v", client)
	_, err := client.EnterRequest(context.Background(), n.getClientInfo()) //todo: this is where the program tends to fail because of issues with contacting the server of the of the other nodes
	if err != nil {
		log.Printf("Failed to send reuquest %v", err)
		return
	}
	defer wg.Done() //Tells the WaitGroup that this go routines is done (decrements the group with one)
}

// EnterRequest simulates behavior of node receiving an EnterRequest() by either putting the requestee in a queue or replying okay.
func (s *NodeServer) EnterRequest(ctx context.Context, in *proto.Client) (*proto.Empty, error) {
	s.Node.updateLamportOnReceive(in.LamportClock) //updates the local lamport clock on receiving the reply and increments it

	if s.Node.State == "HELD" || s.Node.State == "WANTED" && s.Node.Lamport < in.LamportClock { //todo: handle if the lamport clocks are the same e.g. the one with the lowest ID comes first
		s.Node.Queue = append(s.Node.Queue, in) //puts the requestee in the queue to reply after exiting the critical section itself
	} else { //replying okay
		s.Node.incrementLamportClock() //increments the local lamport clock as it is about to reply
		reply := &proto.ReplyOk{
			NodeID:       s.Node.NodeId,
			LamportClock: s.Node.Lamport,
		}
		_, err := s.Node.Node.ReplyOkay(context.Background(), reply) //todo: replace Node name with something else?
		if err != nil {
			log.Printf("Failed to reply okay %v", err)
			return &proto.Empty{}, err
		}
	}
	return &proto.Empty{}, nil
}

// EnterCriticalSection simulates the node entering the critical section with a state change and a log print.
func (n *Node) EnterCriticalSection() {
	n.incrementLamportClock() //increments the local lamport clock as it is about to enter the critical section
	n.State = "HELD"
	log.Printf("Node %d is entering the Critical Section\n", n.NodeId)
	time.Sleep(5 * time.Second)
}

// ExitCriticalSection simulates a node exiting the critical section with a state change and a log print.
// It also sends a reply okay to all the nodes in the queue, then clears the queue.
func (n *Node) ExitCriticalSection() {
	n.incrementLamportClock() //increments the local lamport clock as it is about to exit the critical section
	log.Printf("Node %d is exiting the Critical Section\n", n.NodeId)
	n.State = "RELEASED"
	n.OkReceived = make(map[int32]bool)
	for _, queued := range n.Queue {
		n.incrementLamportClock() //increments the local lamport clock as it is about to reply
		client := n.OtherNodes[queued.Address]
		_, err := client.ReplyOkay(context.Background(), &proto.ReplyOk{NodeID: queued.Id, LamportClock: queued.LamportClock})
		if err != nil {
			log.Printf("Failed to reply ok from queue %v", err)
			return
		}
	}
	n.Queue = nil //clears the queue as it should be empty after replying to all nodes on it.
}

// ReplyOkay handles receiving a reply okay from another node by updating the lamport clock and marking the node as having replied
func (s *NodeServer) ReplyOkay(ctx context.Context, in *proto.ReplyOk) (*proto.Empty, error) {
	s.Node.updateLamportOnReceive(in.LamportClock) //updates the local lamport clock on receiving the reply and increments it
	s.Node.OkReceived[in.NodeID] = true
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
