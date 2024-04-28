package main

type NodeList interface {
	Nodes() []Node
	Register(node Node)
	Remove(node Node)
}
