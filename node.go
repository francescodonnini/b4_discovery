package main

import "fmt"

type Node struct {
	Ip   string
	Port int
}

func (n Node) Address() string {
	return fmt.Sprintf("%s:%d", n.Ip, n.Port)
}
