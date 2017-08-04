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
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/metalnem/dropbox/hash"
)

const usage = `usage: hashes create dir1 dir2 ...
       hashes verify file1 file2`

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

type database struct {
	time   int64
	byHash map[string][]string
}

func computeHash(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)

	if err != nil {
		return "", err
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
			return "", err
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
				ch <- fileInfo{err: err}
			}
		}

		close(ch)
	}()

	return ch
}

func createDb(ctx context.Context, files []fileInfo) error {
	name := fmt.Sprintf("%d.hashes", time.Now().Unix())
	db, err := sql.Open("sqlite3", name)

	if err != nil {
		return err
	}

	defer db.Close()

	if _, err = db.ExecContext(ctx, schema); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, "insert into files(path, hash) values(?, ?)")

	if err != nil {
		tx.Rollback()
		return err
	}

	defer stmt.Close()

	for path, hash := range files {
		if _, err := stmt.ExecContext(ctx, path, hash); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func loadDb(ctx context.Context, path string) ([]fileInfo, error) {
	db, err := sql.Open("sqlite3", path)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	rows, err := db.QueryContext(ctx, "select path, hash from files")

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var files []fileInfo

	for rows.Next() {
		var path, hash string

		if err := rows.Scan(&path, &hash); err != nil {
			return nil, err
		}

		files = append(files, fileInfo{path: path, hash: hash})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
}

func loadFile(ctx context.Context, path string) (*database, error) {
	var time int64

	if _, err := fmt.Sscanf(filepath.Base(path), "%d.hashes", &time); err != nil {
		return nil, err
	}

	files, err := loadDb(ctx, path)

	if err != nil {
		return nil, err
	}

	byHash := make(map[string][]string)

	for _, file := range files {
		byHash[file.hash] = append(byHash[file.hash], file.path)
	}

	return &database{time: time, byHash: byHash}, nil
}

func create(ctx context.Context, dirs []string) error {
	var files []fileInfo

	for file := range computeHashes(ctx, dirs) {
		if file.err != nil {
			return file.err
		}

		fmt.Println(file.path)
		files = append(files, file)
	}

	return createDb(ctx, files)
}

func verify(ctx context.Context, file1, file2 string) error {
	db1, err := loadFile(ctx, file1)

	if err != nil {
		return err
	}

	db2, err := loadFile(ctx, file2)

	if err != nil {
		return err
	}

	if db1.time > db2.time {
		db1, db2 = db2, db1
	}

	for hash, paths := range db1.byHash {
		if _, ok := db2.byHash[hash]; !ok {
			for _, path := range paths {
				fmt.Println(path)
			}
		}
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println(usage)
		return
	}

	ctx := context.Background()
	action := strings.ToLower(os.Args[1])
	params := os.Args[2:]

	if action == "create" && len(params) > 0 {
		if err := create(ctx, params); err != nil {
			log.Fatal(err)
		}
	} else if action == "verify" && len(params) == 2 {
		if err := verify(ctx, params[0], params[1]); err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println(usage)
	}
}
