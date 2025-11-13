package utils

import (
	"fmt"
	"time"
)

func Log(message string) {
	now := time.Now().Truncate(time.Second)
	logMessage := fmt.Sprintf("[%s] %s", now.Format("2006-01-02 15:04:05"), message)
	fmt.Println(logMessage)
}
