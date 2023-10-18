package db

import (
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pe-gomes/codepix-go/domain/model"

	"github.com/jinzhu/gorm"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	_ "gorm.io/driver/sqlite"
)

func init() {
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)

	err := godotenv.Load(basepath + "/../../.env")

	if err != nil {
		log.Fatalf("Error loading .env files")
	}
}

func ConnectDB(env string) *gorm.DB {
	var dns string
	var db *gorm.DB
	var err error

	if env != "test" {
		dns = os.Getenv("dns")
		db, err = gorm.Open(os.Getenv("dbType"), dns)
	} else {
		dns = os.Getenv("dnsTest")
		db, err = gorm.Open(os.Getenv("dbTypeTest"), dns)
	}

	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
		panic(err)
	}

	if os.Getenv("debug") == "true" {
		db.LogMode(true)
	}

	if os.Getenv("AutoMigrateDb") == "true" {
		db.AutoMigrate(&model.Bank{}, &model.Account{}, &model.PixKey{}, &model.Transaction{})
	}

	return db
}
