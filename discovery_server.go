package main

import (
	"context"
	"github.com/francescodonnini/discovery_grpc/pb"
	event_bus "github.com/francescodonnini/pubsub"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Service struct {
	pb.UnimplementedDiscoveryServer
	mu    *sync.RWMutex
	nodes map[string]*Descriptor
	bus   *event_bus.EventBus
}

func NewDiscoveryService(bus *event_bus.EventBus) *Service {
	return &Service{
		mu:    &sync.RWMutex{},
		nodes: make(map[string]*Descriptor),
		bus:   bus,
	}
}

func (d *Service) Remove(node Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.nodes, node.Address())
	log.Printf("%s exited!\n", node.Address())
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
		d.bus.Publish(event_bus.Event{
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
