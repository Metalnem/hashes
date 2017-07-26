package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const schema = `create table files (
	path text not null primary key,
	hash text not null
);`

func create() error {
	name := fmt.Sprintf("%d.hashes", time.Now().Unix())
	db, err := sql.Open("sqlite3", name)

	if err != nil {
		return errors.Wrapf(err, "failed to create database file %s", name)
	}

	defer db.Close()

	if _, err = db.Exec(schema); err != nil {
		return errors.Wrap(err, "failed to initialize database")
	}

	return nil
}

func main() {
	if err := create(); err != nil {
		log.Fatal(err)
	}
}
