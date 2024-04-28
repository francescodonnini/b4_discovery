package main

import (
	"context"
	"github.com/francescodonnini/discovery_grpc/pb"
	"google.golang.org/protobuf/types/known/emptypb"
	"math/rand"
	"time"
)

type Service struct {
	pb.UnimplementedDiscoveryServer
	nodes NodeList
}

func NewDiscoveryService(nodes *Heartbeat) *Service {
	return &Service{
		nodes: nodes,
	}
}

func (d *Service) Join(_ context.Context, in *pb.Node) (*pb.NodeList, error) {
	node := Node{
		Ip:   in.Ip,
		Port: int(in.Port),
	}
	d.nodes.Register(node)
	peers := make([]*pb.Node, 0)
	for _, p := range d.nodes.Nodes() {
		if p.Address() != node.Address() {
			peers = append(peers, &pb.Node{Ip: p.Ip, Port: int32(p.Port)})
		}
	}
	rand.NewSource(time.Now().Unix())
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
	return &pb.NodeList{Peers: peers}, nil
}

func (d *Service) Exit(_ context.Context, in *pb.Node) (*emptypb.Empty, error) {
	d.nodes.Remove(Node{
		Ip:   in.Ip,
		Port: int(in.Port),
	})
	return &emptypb.Empty{}, nil
}
