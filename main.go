package main

import (
	"context"
	"fmt"
	"github.com/francescodonnini/discovery_grpc/pb"
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
	port, err := strconv.ParseInt(os.Getenv("PORT"), 10, 16)
	if err != nil {
		panic("No port provided.")
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %s\n", err)
	}
	srv := NewHeartbeatService(Node{
		Ip:   "0.0.0.0",
		Port: int(port),
	}, 6)
	go startGrpcSrv(lis, srv)
	go startUdpSrv(srv)
	ticker := time.NewTicker(5000 * time.Millisecond)
	for range ticker.C {
		srv.OnTimeout()
	}
}

func startGrpcSrv(lis net.Listener, heartbeat *Heartbeat) {
	server := grpc.NewServer()
	reflection.Register(server)
	disc := NewDiscoveryService(heartbeat)
	pb.RegisterDiscoveryServer(server, disc)
	if err := server.Serve(lis); err != nil {
		log.Printf("Failed to serve: %v\n", err)
	}
}

func startUdpSrv(srv *Heartbeat) {
	srv.Serve(context.Background())
}
