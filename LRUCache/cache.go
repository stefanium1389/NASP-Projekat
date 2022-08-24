package LRUCache

import (
	//"fmt"
	"container/list"
)

type Node struct{
	key string
	value []byte
}

type Cache struct{
	capacity int
	list *list.List
	hashmap map[string] *list.Element
}

func CacheConstructor (capacity int) Cache{
	cache := Cache{}
	cache.capacity = capacity
	cache.list = list.New()
	cache.hashmap = make(map[string]*list.Element, capacity)

	return cache
}

func (cache *Cache) Add(key string, value []byte){
	element, exists := cache.hashmap[key]
	if exists{
		cache.list.MoveToBack(element)

		//if updated
		//cache.hashmap[key] = node
		//cache.list.Back().Value = node.Value
	}else{
		//cache is full -> remove LRU (front of list)
		if cache.capacity == cache.list.Len(){
			cache.RemoveLRU()
		}

		node := &list.Element{
			Value: Node{
				key: key,
				value: value,
			},
		}
		cache.list.PushBack(node.Value)
		cache.hashmap[key] = node
	}
}

func (cache *Cache) RemoveLRU(){
	lru := cache.list.Front()
	cache.list.Remove(lru)
	delete(cache.hashmap, lru.Value.(Node).key)
}

func (cache *Cache) Get(key string) (bool, []byte){
	element, exists := cache.hashmap[key]
	if exists{
		cache.list.MoveToBack(element)
		return true, element.Value.(Node).value
	}
	return false, nil
}


