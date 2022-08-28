package SkipList

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

const (
	DefaultMaxLevel    int     = 15   //Maximal level allow to create in this skip list
	//DefaultProbability float32 = 0.25 //Default Probability
)

type Skipnode struct {
	Key     string
	Value    []byte
	Tombstone bool
	Forward []*Skipnode
	Level   int
}

type Skiplist struct {
	Header *Skipnode
	MaxLevel    int
	Probability float32
	Level int
}

func NewNode(searchKey string, value []byte, createLevel int, maxLevel int) *Skipnode {
	forwardEmpty := make([]*Skipnode, maxLevel)
	for i := 0; i <= maxLevel-1; i++ {
		forwardEmpty[i] = nil
	}
	return &Skipnode{Key: searchKey, Value: value, Forward: forwardEmpty, Level: createLevel}
}

func NewSkipList(maxLevel int, probability float32) *Skiplist {
	newList := &Skiplist{Header: NewNode("", nil, 1, maxLevel), Level: 1}
	newList.MaxLevel = maxLevel       //default
	newList.Probability = probability //default
	return newList
}


func randomP() float32 {
	rand.Seed(int64(time.Now().Nanosecond()))
	return rand.Float32()
}


func (b *Skiplist) SetMaxLevel(maxLevel int) {
	b.MaxLevel = maxLevel
}
func NewHead() *Skipnode {
	return NewNode( "", nil, 0, DefaultMaxLevel)
}

func (b *Skiplist) Empty() {
	b.Header = NewHead()
	b.Level = 1
}

func (b *Skiplist) RandomLevel() int {
	level := 1
	for randomP() < b.Probability && level < b.MaxLevel {
		level++
	}
	return level
}


func (b *Skiplist) Search(searchKey string) (*Skipnode,error) {
	currentNode := b.Header

	for i := b.Level - 1; i >= 0; i-- {
		for currentNode.Forward[i] != nil && currentNode.Forward[i].Key < searchKey {
			currentNode = currentNode.Forward[i]
		}
	}

	currentNode = currentNode.Forward[0]

	if currentNode != nil && currentNode.Key == searchKey {
		return currentNode, nil
	}
	return nil, errors.New("Not found.")
}

func (b *Skiplist) Insert(searchKey string, value []byte) bool {
	updateList := make([]*Skipnode, b.MaxLevel)
	currentNode := b.Header

	for i := b.Header.Level - 1; i >= 0; i-- {
		for currentNode.Forward[i] != nil && currentNode.Forward[i].Key < searchKey {
			currentNode = currentNode.Forward[i]
		}
		updateList[i] = currentNode
	}
	currentNode = currentNode.Forward[0]

	if currentNode != nil && currentNode.Key == searchKey {
		currentNode.Value = value
	} else {
		newLevel := b.RandomLevel()
		if newLevel > b.Level {
			for i := b.Level + 1; i <= newLevel; i++ {
				updateList[i-1] = b.Header
			}
			b.Level = newLevel
			b.Header.Level = newLevel
		}

		newNode := NewNode(searchKey, value, newLevel, b.MaxLevel)
		for i := 0; i <= newLevel-1; i++ {
			newNode.Forward[i] = updateList[i].Forward[i]
			updateList[i].Forward[i] = newNode
		}
	}
	return true
}

func (b *Skiplist) Delete(searchKey string) error {
	currentNode := b.Header

	for i := b.Header.Level - 1; i >= 0; i-- {
		for currentNode.Forward[i] != nil && currentNode.Forward[i].Key < searchKey {
			currentNode = currentNode.Forward[i]
		}
	}

	currentNode = currentNode.Forward[0]

	if currentNode.Key == searchKey {
		for i := 0; i <= currentNode.Level-1; i++ {
			currentNode.Tombstone = true
			
		}

		for currentNode.Level > 1 && b.Header.Forward[currentNode.Level] == nil {
			currentNode.Level--
		}

		currentNode = nil
		return nil
	}
	return errors.New("Not found")
}

func (b *Skiplist) GetAllElements() []*Skipnode {
	current := b.Header
	res := make([]*Skipnode, 0)
	current = current.Forward[0]
	res = append(res, current)
	for i := 1; i < b.MaxLevel; i++ {
		current = current.Forward[0]
		res = append(res, current)
	}
	return res
}
func (b *Skiplist) DisplayAll() {
	fmt.Printf("\nhead->")
	currentNode := b.Header

	for {
		fmt.Printf("[key:%d][val:%v]->", currentNode.Key, currentNode.Value)
		if currentNode.Forward[0] == nil {
			break
		}
		currentNode = currentNode.Forward[0]
	}
	fmt.Printf("nil\n")

	fmt.Println("---------------------------------------------------------")
	currentNode = b.Header
	//Draw all data node.
	for {
		fmt.Printf("[node:%d], val:%v, level:%d ", currentNode.Key, currentNode.Value, currentNode.Level)

		if currentNode.Forward[0] == nil {
			break
		}

		for j := currentNode.Level - 1; j >= 0; j-- {
			fmt.Printf(" fw[%d]:", j)
			if currentNode.Forward[j] != nil {
				fmt.Printf("%d", currentNode.Forward[j].Key)
			} else {
				fmt.Printf("nil")
			}
		}
		fmt.Printf("\n")
		currentNode = currentNode.Forward[0]
	}
	fmt.Printf("\n")
}
