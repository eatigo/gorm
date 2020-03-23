package tests

import (
"fmt"
"os"
"path/filepath"

"github.com/eatigo/gorm"
_ "github.com/eatigo/gorm/dialects/mssql"
_ "github.com/eatigo/gorm/dialects/mysql"
_ "github.com/eatigo/gorm/dialects/postgres"
_ "github.com/eatigo/gorm/dialects/sqlite"
)

// OpenTestConnection open test connection used for GORM testing
func OpenTestConnection() (db *gorm.DB, err error) {
	switch os.Getenv("GORM_DIALECT") {
	case "mysql":
		// CREATE USER 'gorm'@'localhost' IDENTIFIED BY 'gorm';
		// CREATE DATABASE gorm;
		// GRANT ALL ON gorm.* TO 'gorm'@'localhost';
		fmt.Println("testing mysql...")
		dbhost := os.Getenv("GORM_DBADDRESS")
		if dbhost != "" {
			dbhost = fmt.Sprintf("tcp(%v)", dbhost)
		}
		db, err = gorm.Open("mysql", fmt.Sprintf("gorm:gorm@%v/gorm?charset=utf8&parseTime=True", dbhost))
	case "postgres":
		fmt.Println("testing postgres...")
		dbhost := os.Getenv("GORM_DBHOST")
		if dbhost != "" {
			dbhost = fmt.Sprintf("host=%v ", dbhost)
		}
		db, err = gorm.Open("postgres", fmt.Sprintf("%v user=gorm password=gorm DB.name=gorm sslmode=disable", dbhost))
	case "foundation":
		fmt.Println("testing foundation...")
		db, err = gorm.Open("foundation", "dbname=gorm port=15432 sslmode=disable")
	case "mssql":
		// CREATE LOGIN gorm WITH PASSWORD = 'LoremIpsum86';
		// CREATE DATABASE gorm;
		// USE gorm;
		// CREATE USER gorm FROM LOGIN gorm;
		// sp_changedbowner 'gorm';
		fmt.Println("testing mssql...")
		db, err = gorm.Open("mssql", "sqlserver://gorm:LoremIpsum86@localhost:1433?database=gorm")
	default:
		fmt.Println("testing sqlite3...")
		db, err = gorm.Open("sqlite3", filepath.Join(os.TempDir(), "gorm.db"))
	}

	// db.SetLogger(Logger{log.New(os.Stdout, "\r\n", 0)})
	// db.SetLogger(log.New(os.Stdout, "\r\n", 0))
	if os.Getenv("DEBUG") == "true" {
		db.LogMode(true)
	}

	if err == nil {
		db.DB().SetMaxIdleConns(10)
	}

	return
}