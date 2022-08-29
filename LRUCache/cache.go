package LRUCache

import (
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

func NewCache(capacity int) *Cache{
	cache := Cache{}
	cache.capacity = capacity
	cache.list = list.New()
	cache.hashmap = make(map[string]*list.Element, capacity)

	return &cache
}

func (cache *Cache) Add(key string, value []byte){
	element, exists := cache.hashmap[key]

	node := Node {
		key: key,
		value: value,
	}

	if exists{
		cache.list.MoveToBack(element)

	}else{
		//cache is full -> remove LRU (front of list)
		if cache.capacity == cache.list.Len(){
			cache.RemoveLRU()
		}
		newElement := cache.list.PushBack(node)
		cache.hashmap[key] = newElement
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

func(cache *Cache) Remove(key string) bool{
	element, exists := cache.hashmap[key]
	if exists{
		cache.list.Remove(element)
		return true
	}
	return false
}
