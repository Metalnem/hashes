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

func computeHash(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)

	if err != nil {
		return "", errors.Wrapf(err, "failed to open file %s", path)
	}

	defer f.Close()

	h := hash.New()

	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}

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

func create(ctx context.Context, dirs []string) error {
	name := fmt.Sprintf("%d.hashes", time.Now().Unix())
	db, err := sql.Open("sqlite3", name)

	if err != nil {
		return errors.Wrapf(err, "failed to create database file %s", name)
	}

	defer db.Close()

	if _, err = db.ExecContext(ctx, schema); err != nil {
		return errors.Wrap(err, "failed to initialize database")
	}

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

			h, err := computeHash(ctx, path)

			if err != nil {
				return err
			}

			fmt.Printf("%s %s\n", h, path)

			return nil
		})

		if err != nil {
			return errors.Wrapf(err, "failed to traverse directory %s", dir)
		}
	}

	return nil
}

func main() {
	ctx := context.Background()
	dirs := os.Args[1:]

	if err := create(ctx, dirs); err != nil {
		log.Fatal(err)
	}
}
