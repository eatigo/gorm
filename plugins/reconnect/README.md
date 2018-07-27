# AutoReconnect

GORM Auto Reconnect Plugin

## Usage


```go
import "github.com/eatigo/gorm/plugins/reconnect"

func main() {
  DB, err := gorm.Open("mysql", "my-dsn")
  Reconnect := reconnect.New(&reconnect.Config{
    Attempts: 3,
    Interval: 3 * time.Second,
  })

  DB.Use(Reconnect)
}
```
