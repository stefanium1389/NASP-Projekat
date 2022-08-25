package main

import (
	"bufio"
	"crypto/sha1"
	"os"
)

type Node struct {
	data  []byte
	left  *Node
	right *Node
	real  bool
}

func Load(filePath string) []Node {
	nodes := []Node{}

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	} else {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			nodes = append(nodes, Node{data: []byte(scanner.Text()), real: true})
		}
	}

	return nodes
}

func Upload(nodes []Node) {
	file, err := os.Create("Metadata.txt")
	if err != nil {
		panic(err)
	} else {
		defer file.Close()
		for i := 0; i < len(nodes); i++ {
			if nodes[i].real == true {
				file.WriteString(string(nodes[i].data) + "\n")
			}
		}
	}
}

func Process(nodes []Node) *Node {
	if len(nodes) > 1 && len(nodes)%2 == 1 {
		nodes = append(nodes, Node{data: []byte{}, real: false})
	}

	for i := 0; i < len(nodes); i++ {
		temp := sha1.Sum(nodes[i].data)
		nodes[i].data = temp[:]
	}

	if len(nodes) == 1 {
		return &nodes[0]
	} else {
		parents := []Node{}
		for i := 0; i < len(nodes); i += 2 {
			parents = append(parents, Node{data: append(nodes[i].data, nodes[i+1].data...), left: &nodes[i], right: &nodes[i+1], real: true})
		}
		return Process(parents)
	}
}

func Sort_Place(nodes *[]Node, node *Node, father int, son int) {
	(*nodes)[2*father+son] = *node
	if node.left != nil {
		Sort_Place(nodes, node.left, 2*father+son, 1)
	}
	if node.right != nil {
		Sort_Place(nodes, node.right, 2*father+son, 2)
	}
}

func Sort(root *Node) []Node {
	size := 1
	for temp := root; temp != nil; temp = temp.left {
		size *= 2
	}

	nodes := []Node{}
	for i := 1; i <= size; i++ {
		nodes = append(nodes, Node{})
	}

	Sort_Place(&nodes, root, 0, 0)

	return nodes
}

func main() {
	nodes := Load("Data.txt")
	root := Process(nodes)
	sorted := Sort(root)
	Upload(sorted)
}
