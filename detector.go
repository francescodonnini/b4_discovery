package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/francescodonnini/bus"
	"log"
	"net"
	"strings"
	"sync"
)

type Heartbeat struct {
	addr           Node
	mu             *sync.RWMutex
	beats          map[string]*Descriptor
	rounds         uint64
	maxNumOfRounds uint64
	bus            *bus.EventBus
}

func NewHeartbeatService(address Node, threshold uint64, bus *bus.EventBus) *Heartbeat {
	return &Heartbeat{
		addr:           address,
		mu:             new(sync.RWMutex),
		beats:          make(map[string]*Descriptor),
		rounds:         0,
		maxNumOfRounds: threshold,
		bus:            bus,
	}
}

func (s *Heartbeat) Add(node Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beats[node.Ip] = &Descriptor{node, 0, 0}
}

func (s *Heartbeat) OnTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rounds += 1
	for addr, desc := range s.beats {
		if s.rounds-desc.LastUpdated >= s.maxNumOfRounds {
			s.bus.Publish(bus.Event{
				Topic:   "exit",
				Content: desc.Node,
			})
			log.Printf("%s exited!\n", desc.Address())
			delete(s.beats, addr)
		}
	}
}

func (s *Heartbeat) Serve(ctx context.Context) {
	srv, err := net.ListenPacket("udp", s.addr.Address())
	if err != nil {
		log.Fatalf("Cannot listen to %s. Error: %s\n", s.addr.Address(), err)
	}
	go func() {
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()
		buf := make([]byte, 65536)
		for {
			n, snd, err := srv.ReadFrom(buf)
			if err != nil {
				log.Printf("cannot read from udp socket. error: %s\n", err)
				continue
			}
			msg, err := s.decodeBeat(buf[:n])
			if err != nil {
				log.Printf("%s\n", msg)
				continue
			}
			log.Printf("received heart beat from %s\n", snd.String())
			i := strings.LastIndex(snd.String(), ":")
			s.updateBeat(snd.String()[:i])
		}
	}()
}

func (s *Heartbeat) decodeBeat(payload []byte) (Beat, error) {
	var msg Beat
	dec := gob.NewDecoder(bytes.NewReader(payload))
	err := dec.Decode(&msg)
	if err != nil {
		return Beat{}, fmt.Errorf("cannot decode beat with error: %s\n", err)
	}
	return msg, err
}

func (s *Heartbeat) updateBeat(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	desc, ok := s.beats[address]
	if ok {
		desc.Tick += 1
		desc.LastUpdated = s.rounds
	}
}
