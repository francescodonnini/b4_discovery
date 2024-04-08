package main

import (
	"context"
	"github.com/francescodonnini/bus"
	"github.com/francescodonnini/discovery_grpc/pb"
	"google.golang.org/protobuf/types/known/emptypb"
	"math/rand"
	"sync"
	"time"
)

type Service struct {
	pb.UnimplementedDiscoveryServer
	mu    *sync.RWMutex
	nodes map[string]*Descriptor
	bus   *bus.EventBus
}

func NewDiscoveryService(bus *bus.EventBus) *Service {
	return &Service{
		mu:    &sync.RWMutex{},
		nodes: make(map[string]*Descriptor),
		bus:   bus,
	}
}

func (d *Service) Remove(node Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bus.Publish(bus.Event{Topic: "exit", Content: node})
	delete(d.nodes, node.Address())
}

func (d *Service) Join(_ context.Context, in *pb.Node) (*pb.NodeList, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	node := Node{
		Ip:   in.Ip,
		Port: int(in.Port),
	}
	_, ok := d.nodes[node.Address()]
	if !ok {
		d.bus.Publish(bus.Event{
			Topic:   "join",
			Content: node,
		})
		d.nodes[node.Address()] = &Descriptor{node, 0, 0}
	}
	peers := make([]*pb.Node, 0)
	for _, p := range d.nodes {
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
	d.mu.Lock()
	defer d.mu.Unlock()
	node := Node{Ip: in.Ip, Port: int(in.Port)}
	delete(d.nodes, node.Address())
	return &emptypb.Empty{}, nil
}
