package MerkleTree

import (
	"crypto/sha1"
	"encoding/hex"
	"os"
)

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
	real  bool
}

type Root struct {
	Root *Node
}

func BuildMerkle(parts []Node) *Node {
	nodes := []Node{}
	i := 0
	for ; i < len(parts); i += 2 {
		if (i + 1) < len(parts) {
			nodes = append(nodes, Node{left: &parts[i], right: &parts[i+1], data: Hash(parts[i], parts[i+1])})
		} else {
			nodes = append(nodes, Node{left: &parts[i], right: &Node{}, data: parts[i].data})
		}
	}
	if len(nodes) == 1 {
		return &nodes[0]
	} else if len(nodes) > 1 {
		return BuildMerkle(nodes)
	} else {
		panic("Kako si dosao ovde?")
	}
}

func Upload(nodes []Node) {
	file, err := os.Create("Metadata.txt")
	if err != nil {
		panic(err)
	} else {
		defer file.Close()
		for i := 0; i < len(nodes); i++ {
			if nodes[i].real == true {
				//file.WriteString(string(nodes[i].data) + "\n")
			}
		}
	}
}

func Process(parts [][20]byte) *Node {
	var nodes []Node
	i := 0
	for ; i < len(parts); i++ {
		nodes = append(nodes, Node{left: &Node{}, right: &Node{}, data: parts[i]})
	}
	return BuildMerkle(nodes)
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

func Preorder(root *Node, file *os.File) {
	if root != nil {
		file.WriteString(hex.EncodeToString(root.data[:]) + "\n")
		Preorder(root.left, file)
		Preorder(root.right, file)
	}
}

func Hash(node1, node2 Node) [20]byte {
	var left, right [20]byte
	left = node1.data
	right = node2.data
	return sha1.Sum(append(left[:], right[:]...))
}

func HashData(data []byte) [20]byte {
	return sha1.Sum(data)
}
