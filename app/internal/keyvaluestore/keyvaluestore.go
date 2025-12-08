package keyvaluestore

import (
	"fmt"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type KeyStore map[string]KeyStoreValue

var keyStore = make(KeyStore)

type KeyStoreValue struct {
	Key              string
	Value            string
	InsertedDatetime time.Time
	Expire           *time.Time // Optional: nil if not set
}

func Append(value KeyStoreValue) {
	utils.Log(fmt.Sprintf("(KeyValueStore) Append: key = %s, value = %s", value.Key, value.Value))
	keyStore[value.Key] = value
}

func Get(key string) (KeyStoreValue, bool) {
	get, found := keyStore[key]

	if get.Expire != nil && time.Now().After(*get.Expire) {
		utils.Log(fmt.Sprintf("(KeyValueStore) Get key = %s expired", key))
		delete(keyStore, key)
		return KeyStoreValue{}, false
	}

	utils.Log(fmt.Sprintf("(KeyValueStore) Get key = %s, value = %s", key, get.Value))
	return get, found
}
