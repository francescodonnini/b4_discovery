package main

import (
	"context"
	"github.com/francescodonnini/discovery_grpc/pb"
	"github.com/francescodonnini/pubsub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func main() {
	if enabled, err := strconv.ParseBool(os.Getenv("LOGGING_ENABLED")); err != nil || enabled == false {
		log.SetOutput(io.Discard)
	}
	eventBus := event_bus.NewEventBus()
	lis, err := net.Listen("tcp", "0.0.0.0:5050")
	if err != nil {
		log.Fatalf("Failed to listen: %s\n", err)
	}
	srv := NewHeartbeatService(Node{
		Ip:   "0.0.0.0",
		Port: 5050,
	}, 6, eventBus)
	go startGrpcSrv(lis, eventBus)
	go startUdpSrv(srv, eventBus)
	ticker := time.NewTicker(5000 * time.Millisecond)
	for range ticker.C {
		srv.OnTimeout()
	}
}

func startGrpcSrv(lis net.Listener, bus *event_bus.EventBus) {
	server := grpc.NewServer()
	reflection.Register(server)
	disc := NewDiscoveryService(bus)
	pb.RegisterDiscoveryServer(server, disc)
	deathLis := bus.Subscribe("exit")
	go func() {
		for e := range deathLis {
			node := e.Content.(Node)
			disc.Remove(node)
		}
	}()
	if err := server.Serve(lis); err != nil {
		log.Printf("Failed to serve: %v\n", err)
	}
}

func startUdpSrv(srv *Heartbeat, bus *event_bus.EventBus) {
	joinLis := bus.Subscribe("join")
	go func() {
		for e := range joinLis {
			node := e.Content.(Node)
			srv.Add(node)
			log.Printf("%s joined!\n", node.Address())
		}
	}()
	srv.Serve(context.Background())
}
