package reconnect

import (
	"database/sql/driver"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/eatigo/gorm"
)

var _ gorm.PluginInterface = &Reconnect{}

// Reconnect GORM reconnect plugin
type Reconnect struct {
	Config Config
	mutex  sync.Mutex
}

// Config reconnect config
type Config struct {
	Attempts       int
	Interval       time.Duration
	BadConnChecker func(errors []error) bool
	DSN            string
}

// New initialize GORM reconnect DB
func New(config Config) (*Reconnect, error) {
	if config.DSN == "" {
		return nil, errors.New("DSN must not be empty")
	}

	if config.BadConnChecker == nil {
		badConnectRegexp := regexp.MustCompile("(getsockopt: connection refused|invalid connection)$")
		config.BadConnChecker = func(errors []error) bool {
			for _, err := range errors {
				if err == driver.ErrBadConn || badConnectRegexp.MatchString(err.Error()) /* for mysql */ {
					return true
				}
			}
			return false
		}
	}

	if config.Attempts == 0 {
		config.Attempts = 5
	}

	if config.Interval == 0 {
		config.Interval = 5 * time.Second
	}

	return &Reconnect{
		mutex:  sync.Mutex{},
		Config: config,
	}, nil
}

// Apply apply reconnect to GORM DB instance
func (reconnect *Reconnect) Apply(db *gorm.DB) {
	db.Callback().Create().After("gorm:commit_or_rollback_transaction").
		Register("gorm:plugins:reconnect", reconnect.generateCallback(gorm.CreateCallback))
	db.Callback().Update().After("gorm:commit_or_rollback_transaction").
		Register("gorm:plugins:reconnect", reconnect.generateCallback(gorm.UpdateCallback))
	db.Callback().Delete().After("gorm:commit_or_rollback_transaction").
		Register("gorm:plugins:reconnect", reconnect.generateCallback(gorm.DeleteCallback))
	db.Callback().Query().After("gorm:query").
		Register("gorm:plugins:reconnect", reconnect.generateCallback(gorm.QueryCallback))
	db.Callback().RowQuery().After("gorm:row_query").
		Register("gorm:plugins:reconnect", reconnect.generateCallback(gorm.RowQueryCallback))
}

func (reconnect *Reconnect) generateCallback(callbackType gorm.CallbackType) func(*gorm.Scope) {
	return func(scope *gorm.Scope) {
		if scope.HasError() {
			if db := scope.DB(); reconnect.Config.BadConnChecker(db.GetErrors()) {
				reconnect.mutex.Lock()

				connected := db.DB().Ping() == nil

				if !connected {
					for i := 0; i < reconnect.Config.Attempts; i++ {
						if err := reconnect.reconnectDB(scope); err == nil {
							connected = true
							break
						}
						time.Sleep(reconnect.Config.Interval)
					}
				}

				reconnect.mutex.Unlock()

				if connected {
					value := scope.NewDB()
					value.Error = nil
					value.Value = scope.Value
					*scope.DB() = *value
					scope.SQLVars = nil
					scope.CallCallbacks(callbackType)
					scope.SkipLeft()
				}
			}
		}
	}
}

func (reconnect *Reconnect) reconnectDB(scope *gorm.Scope) error {
	var (
		db         = scope.ParentDB()
		sqlDB      = db.DB()
		newDB, err = gorm.Open(db.Dialect().GetName(), reconnect.Config.DSN)
	)

	err = newDB.DB().Ping()

	if err == nil {
		db.Error = nil
		*sqlDB = *newDB.DB()
	}

	return err
}
