package mysql

import (
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type IDB interface {
	GetDB() *gorm.DB
	DBClose() error
	Migrate()
}

type dbRepo struct {
	DB *gorm.DB
}

var repo IDB

func DB() IDB {
	return repo
}

func MustLoad(userName, password, host, database string, maxOpen, maxIdle int, maxLifeTime time.Duration) {
	db, err := dbConnect(userName, password, host, database, maxOpen, maxIdle, maxLifeTime)
	if err != nil {
		log.Fatalln(err)
		return
	}
	repo = &dbRepo{
		DB: db,
	}
}

func dbConnect(user, pass, addr, dbName string, maxOpen, maxIdle int, maxLifeTime time.Duration) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=%t&loc=%s",
		user,
		pass,
		addr,
		dbName,
		true,
		"Local")

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})

	if err != nil {
		return nil, err
	}

	db.Set("gorm:table_options", "CHARSET=utf8mb4")

	oper, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 设置连接池 用于设置最大打开的连接数，默认值为0表示不限制.设置最大的连接数，可以避免并发太高导致连接mysql出现too many connections的错误。
	oper.SetMaxOpenConns(maxOpen)

	// 设置最大连接数 用于设置闲置的连接数.设置闲置的连接数则当开启的一个连接使用完成后可以放在池里等候下一次使用。
	oper.SetMaxIdleConns(maxIdle)

	// 设置最大连接超时
	oper.SetConnMaxLifetime(time.Minute * maxLifeTime)
	return db, nil
}

func (d *dbRepo) GetDB() *gorm.DB {
	return d.DB
}

func (d *dbRepo) DBClose() error {
	sqlDB, err := d.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (d *dbRepo) Migrate() {
	checkTableData()
}

func checkTableData(tables ...interface{}) {
	db := DB().GetDB()
	for _, tb := range tables {
		if db.Migrator().HasTable(tb) == false {
			if err := db.Debug().Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4").
				Migrator().CreateTable(tb); err != nil {
				log.Fatal("创建数据表失败: " + err.Error())
			}
		} else {
			// 已存在的表校验一下是否有新增字段
			if err := db.Debug().Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").AutoMigrate(tb); err != nil {
				log.Fatal("数据库初始化失败: " + err.Error())
			}
		}
	}
}
