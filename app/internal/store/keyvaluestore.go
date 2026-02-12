package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type KeyStore struct {
	mu    sync.RWMutex
	store map[string]KeyStoreValue
}

var keyStore = KeyStore{
	store: map[string]KeyStoreValue{},
}

type KeyStoreValue struct {
	Key              string
	Value            string
	InsertedDatetime time.Time
	Expire           *time.Time // Optional: nil if not set
}

func Append(value KeyStoreValue) {
	keyStore.mu.Lock()
	defer keyStore.mu.Unlock()

	utils.Log(fmt.Sprintf("(KeyValueStore) Append: key = %s, value = %s", value.Key, value.Value))
	keyStore.store[value.Key] = value
}

func Get(key string) (KeyStoreValue, bool) {
	keyStore.mu.RLock()
	get, found := keyStore.store[key]
	keyStore.mu.RUnlock()

	if get.Expire != nil && time.Now().After(*get.Expire) {
		keyStore.mu.Lock()
		utils.Log(fmt.Sprintf("(KeyValueStore) Get key = %s expired", key))
		delete(keyStore.store, key)
		keyStore.mu.Unlock()
		return KeyStoreValue{}, false
	}

	utils.Log(fmt.Sprintf("(KeyValueStore) Get key = %s, value = %s", key, get.Value))
	return get, found
}
