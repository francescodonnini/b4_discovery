package main

import "sync"

type Event struct {
	Topic   string
	Content interface{}
}

type EventBus struct {
	mu          sync.RWMutex
	subscribers map[string][]chan Event
	closed      bool
}

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string][]chan Event),
	}
}

func (p *EventBus) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		for _, sub := range p.subscribers {
			for _, ch := range sub {
				close(ch)
			}
		}
	}
}

func (p *EventBus) Subscribe(topic string) <-chan Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := make(chan Event, 1)
	p.subscribers[topic] = append(p.subscribers[topic], ch)
	return ch
}

func (p *EventBus) Publish(event Event) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return
	}
	for _, ch := range p.subscribers[event.Topic] {
		go func(ch chan Event) {
			ch <- event
		}(ch)
	}
}
