package store

import (
	"fmt"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type SimpleStore[T any] interface {
	// Interface definition for simple store
	Append(value T) int
	Get(key string) (T, bool)
}

// TODO interface definition for blocking store

type ListStoreValue struct {
	Key    string
	Values []string
}

type ListStore struct {
	mu    sync.RWMutex
	store map[string][]string
}

var ListStoreLive = ListStore{
	store: make(map[string][]string),
}

func (ls *ListStore) Append(list ListStoreValue) int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	existingList, found := ls.store[list.Key]
	if found {
		utils.Log(fmt.Sprintf("(ListStore) List %s found, tailing value %s", list.Key, list.Values))
		ls.store[list.Key] = append(existingList, list.Values...)
	} else {
		utils.Log(fmt.Sprintf("(ListStore) List %s not found, making new one and tailing value %s", list.Key, list.Values))
		ls.store[list.Key] = list.Values
	}

	numOfElements := len(ls.store[list.Key])
	return numOfElements
}

func (ls *ListStore) Get(key string) (ListStoreValue, bool) {
	return ListStoreValue{}, false
}
