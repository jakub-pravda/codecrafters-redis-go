package store

import (
	"fmt"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type KeyStore map[string]StorageValue

var keyStore = make(KeyStore)

var notFound = StorageValue{}

type StorageValue struct {
	Key              string
	Value            string
	InsertedDatetime time.Time
}

func Append(value StorageValue) {
	utils.Log(fmt.Sprintf("(KeyValueStore) Append: key = %s, value = %s", value.Key, value.Value))
	keyStore[value.Key] = value
}

func Get(key string) StorageValue {
	get := keyStore[key]
	utils.Log(fmt.Sprintf("(KeyValueStore) Get key = %s, value = %s", key, get.Value))
	return get
}
