package reconnect_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/eatigo/gorm"
	"github.com/eatigo/gorm/plugins/reconnect"
	"github.com/eatigo/gorm/tests"
)

func TestReconnect(t *testing.T) {
	DB, err := tests.OpenTestConnection()
	DB.DB().SetConnMaxLifetime(24 * time.Hour)
	r, _ := reconnect.New(reconnect.Config{DSN: "test"})
	DB.Use(r)

	if err != nil {
		t.Error(err)
	}

	for {
		var user User

		go func() {
			result := DB.Find(&user)
			if result == nil {
				fmt.Println("db is nil")
				fmt.Printf("%#v \n", result)
			}
			if result != nil && result.Error == nil {
				fmt.Printf("Found user's ID: %v\n", user.ID)
			} else {
				fmt.Printf("DB Query Err: %v\n", err)
			}
		}()

		time.Sleep(500 * time.Microsecond)
	}
}

type User struct {
	gorm.Model
	Name string
}
