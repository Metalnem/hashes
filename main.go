package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/metalnem/dropbox/hash"
	"github.com/pkg/errors"
)

const schema = `create table files (
	path text not null primary key,
	hash text not null
);`

var buffer = make([]byte, hash.BlockSize)

type fileInfo struct {
	path string
	hash string
	err  error
}

func computeHash(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)

	if err != nil {
		return "", errors.Wrapf(err, "failed to open file %s", path)
	}

	defer f.Close()

	h := hash.New()

	for {
		n, err := f.Read(buffer)

		if n > 0 {
			h.Write(buffer[:n])
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return "", errors.Wrapf(err, "failed to read file %s", path)
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func computeHashes(ctx context.Context, dirs []string) <-chan fileInfo {
	ch := make(chan fileInfo)

	go func() {
		for _, dir := range dirs {
			err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if info.IsDir() {
					return nil
				}

				name := info.Name()

				if name == "" || name[0] == '.' {
					return nil
				}

				hash, err := computeHash(ctx, path)

				if err != nil {
					return err
				}

				ch <- fileInfo{path: path, hash: hash}

				return nil
			})

			if err != nil {
				ch <- fileInfo{err: errors.Wrapf(err, "failed to traverse directory %s", dir)}
			}
		}

		close(ch)
	}()

	return ch
}

func createDb(ctx context.Context, files map[string]string) error {
	name := fmt.Sprintf("%d.hashes", time.Now().Unix())
	db, err := sql.Open("sqlite3", name)

	if err != nil {
		return errors.Wrapf(err, "failed to create database file %s", name)
	}

	defer db.Close()

	if _, err = db.ExecContext(ctx, schema); err != nil {
		return errors.Wrap(err, "failed to initialize database")
	}

	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}

	stmt, err := tx.PrepareContext(ctx, "insert into files(path, hash) values(?, ?)")

	if err != nil {
		tx.Rollback()
		return errors.Wrap(err, "failed to prepare insert statement")
	}

	defer stmt.Close()

	for path, hash := range files {
		if _, err := stmt.ExecContext(ctx, path, hash); err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "failed to insert hash for %s", path)
		}
	}

	return tx.Commit()
}

func main() {
	ctx := context.Background()
	dirs := os.Args[1:]
	files := make(map[string]string)

	for file := range computeHashes(ctx, dirs) {
		if file.err != nil {
			log.Fatal(file.err)
		}

		fmt.Println(file.path)
		files[file.path] = file.hash
	}

	if err := createDb(ctx, files); err != nil {
		log.Fatal(err)
	}
}
