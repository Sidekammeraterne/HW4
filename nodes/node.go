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
	Queue          []*proto.Client
	OtherNodes     map[string]proto.RicartArgawalaClient
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

// todo: add implementation of lamport clocks everywhere
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
		OtherNodes: make(map[string]proto.RicartArgawalaClient),
	}
	log.Printf("Node struct initialized")

	//setup server part //todo: only done in go function because start_server is blocking and needed to insert wait time afterwards
	go func() {
		server := &NodeServer{
			Node: &n, //pointer to the node object in the server
		}
		server.start_server(configuration.Port)
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
	//Todo: this introduces a race condition has all the nodes will emidiatly request access to the Critical Section
	choice := rand.Intn(10) //returns a random number between 0 and 9
	if choice < 7 {         //if the number is less than 7 (70% chance) it will enter the critical section
		n.Lamport++
		n.State = "WANTED"

		//The WaitGroup keeps track of how many go routines are running
		wg := new(sync.WaitGroup)

		//Sends enter() request to all the other nodes in the system
		for _, other := range n.OtherNodes {
			wg.Add(1)
			go n.sendRequest(other, wg)
			log.Println("SPAWNED GO ROUTINE FOR REQUEST", other)
		}

		wg.Wait() //waits for the go routines to terminate, will continue when all are done simulation ok from everyone
		log.Printf("Node %d is entering the Critical Section\n", n.NodeId)
		time.Sleep(5 * time.Second)
		log.Printf("Node %d is exiting the Critical Section\n", n.NodeId)
		n.Exit()

		//todo: at some point make an exit from the critical section and inform everyone its in the queue - I don't have a good idea for this
	} else { //if the number is 7 or higher (30% chance) it will simply sleep
		time.Sleep(10 * time.Second) //todo: can be changed if we want something else.
	}

}

// Makes rpc call with enter() request to the given node
func (n *Node) sendRequest(client proto.RicartArgawalaClient, wg *sync.WaitGroup) {
	log.Printf("Sending request EnterRequest to %v", client)
	response, err := client.EnterRequest(context.Background(), n.getClientInfo()) //todo: this is where the program tends to fail because of issues with contacting the server of the of the other nodes
	if err != nil {
		log.Printf("did not recieve anything or failed to send %v", err)
		return
	}
	log.Printf("Response: %v from %v", response, client)
	//Terminates the go routine if the response is an okay from the contacted node
	if response.GetOkay() == "ok" {
		defer wg.Done() //Tells the WaitGroup that this go routines is done (decrements the group with one)
		return
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

	log.Printf("[Server] listening on port %s", port)
	err = grpcServer.Serve(listener)
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
	log.SetFlags(log.Ldate | log.Ltime)               //metadata written before each log message todo: include
}

/*
	function for when a node recieves a request to enter the critical section from another node

If this node has the state "HELD", it puts the other node in a queue
If this node has the state "WANTED"
*/
func (s *NodeServer) EnterRequest(ctx context.Context, in *proto.Client) (*proto.Reply, error) {

	if s.Node.State == "HELD" || s.Node.State == "WANTED" && s.Node.Lamport < in.LamportClock { //todo: handle if the lamport clocks are the same e.g. the one with the lowest ID comes first
		s.Node.Queue = append(s.Node.Queue, in)

		//todo: dansk //go routine til når den så selv har exittet
		go s.Node.WaitForReleased() //todo: slet goroutine.
		//return Empty //todo: slet?
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
