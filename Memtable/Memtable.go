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
	threshold   int                // maksimalni kapacitet tj. prag zapisa (kad se dosegne, vrsi se flus\
	currentSize int                // trenutna velicina svih elemenata ukupno

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

func (mt *Memtable) toFlush() bool {
	if mt.threshold <= mt.currentSize {
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
		return mt.Skiplist.Insert(key, value)
	} else {
		toFlush := mt.toFlush()
		if toFlush == false {
			mt.currentSize++
			return mt.Skiplist.Insert(key, value)

		} else {
			return false
		}

	}

}


func (mt *Memtable) Find(key string) *SkipList.Skipnode {
	node := mt.Skiplist.Search(key)
	if node != nil {
		return node
	}
	return nil
}

func (mt *Memtable) FindAndDelete(key string) bool {
	node := mt.Find(key)
	if node != nil {

		if node.Tombstone == false {
			node.Tombstone = true
			deleted := mt.Skiplist.Delete(key)
			if !deleted{
				return false
			}
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

func (mt *Memtable) GetSL() *SkipList.Skiplist {
	return mt.Skiplist
}

func (mt *Memtable) GetThreshold() int {
	return mt.threshold
}



