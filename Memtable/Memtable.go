package Memtable

import (
	"fmt"
	"main/SkipList"
)

//var memTableNum = 0

const (
	maxLevel = 10
)

type Memtable struct {
	Skiplist    *SkipList.Skiplist // podaci
	threshold   int                // maksimalni kapacitet(u bajtovima) tj. prag zapisa (kad se dosegne, vrsi se flus\
	currentSize int                // trenutni broj elemenata

}

func NewMemtable(threshold int, maxLevel int, probability float32) *Memtable {
	return &Memtable{
		Skiplist:    SkipList.NewSkipList(maxLevel, probability),
		threshold:   threshold,
		currentSize: 0,
	}
}

func (mt *Memtable) CurrentSize() int {
	return mt.currentSize
}

func (mt *Memtable) toFlush(dataSize int) bool {
	if mt.threshold <= mt.currentSize + dataSize {
		return true
	} else {
		return false
	}
}

func (mt *Memtable) Insert(key string, value []byte) bool {
	node := mt.Find(key) // ovo je kljucna funkcija
	if node != nil {
		if node.Tombstone == true {
			node.Tombstone = false
			return true
		}
	} else {
		dataSize := len(key) + len(value)
		toFlush := mt.toFlush(dataSize)
		if toFlush == false {
			mt.currentSize += dataSize
			return mt.Skiplist.Insert(key, value)
		}else{
			//Flush()
			mt.currentSize += dataSize
			return mt.Skiplist.Insert(key, value)
		}

	}

	return false
}

func (mt *Memtable) Find(key string) *SkipList.Skipnode {
	node, _ := mt.Skiplist.Search(key)
	if node != nil {
		return node
	}
	return nil
}

func (mt *Memtable) FindAndDelete(key string) bool {
	node := mt.Find(key)
	if node != nil {
		if node.Tombstone == false{
			node.Tombstone = true
		}
		return true
	}
	return false
}

func (mt *Memtable) Empty() {
	mt.currentSize = 0
	mt.Skiplist.Empty()
}

func (mt *Memtable) PrintMt() {
	fmt.Println("Threshold", mt.threshold)
	fmt.Println("Current size of Memtable: ", mt.currentSize)
	mt.Skiplist.DisplayAll()
}

func test() {
	mt := NewMemtable( 20, 15, 0.5)
	mt.Insert("1", []byte("pozdrav1"))
	mt.Insert("2", []byte("pozdrav2"))
	mt.Insert("4", []byte("pozdrav4"))
	mt.Insert("6", []byte("pozdrav6"))
	mt.Insert("5", []byte("pozdrav5"))
	mt.Insert("3", []byte("pozdrav3"))

	node := mt.Find("2")
	fmt.Printf(string(node.Value) + "\n")

	mt.FindAndDelete("6")
	mt.PrintMt()
	node = mt.Find("5")
}

func (mt *Memtable) GetSL() *SkipList.Skiplist {
	return mt.Skiplist
}

func (mt *Memtable) GetThreshold() int {
	return mt.threshold
}
