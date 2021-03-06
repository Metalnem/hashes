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
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/metalnem/dropbox/hash"
)

const usage = `Usage of hashes:
  hashes create dir1 dir2 ...
  hashes diff file1 file2`

const schema = `create table files (
	path text not null primary key,
	hash text not null
);`

var buffer = make([]byte, hash.BlockSize)

type file struct {
	path string
	hash string
	err  error
}

func computeHash(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)

	if err != nil {
		return "", err
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
			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func computeHashes(ctx context.Context, dirs []string) <-chan file {
	ch := make(chan file)

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

				ch <- file{path: path, hash: hash}

				return nil
			})

			if err != nil {
				ch <- file{err: err}
			}
		}

		close(ch)
	}()

	return ch
}

func createDb(ctx context.Context, files []file) error {
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

	for _, file := range files {
		if _, err := stmt.ExecContext(ctx, file.path, file.hash); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func loadDb(ctx context.Context, path string) (map[string][]string, error) {
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

	files := make(map[string][]string)

	for rows.Next() {
		var path, hash string

		if err := rows.Scan(&path, &hash); err != nil {
			return nil, err
		}

		files[hash] = append(files[hash], path)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
}

func missing(files1, files2 map[string][]string) []string {
	var res []string

	for hash, paths := range files1 {
		if _, ok := files2[hash]; !ok {
			res = append(res, paths...)
		}
	}

	sort.Strings(res)

	return res
}

func create(ctx context.Context, dirs []string) error {
	var files []file

	for file := range computeHashes(ctx, dirs) {
		if file.err != nil {
			return file.err
		}

		fmt.Println(file.path)
		files = append(files, file)
	}

	return createDb(ctx, files)
}

func diff(ctx context.Context, file1, file2 string) error {
	files1, err := loadDb(ctx, file1)

	if err != nil {
		return err
	}

	files2, err := loadDb(ctx, file2)

	if err != nil {
		return err
	}

	for _, path := range missing(files1, files2) {
		fmt.Printf("- %s\n", path)
	}

	for _, path := range missing(files2, files1) {
		fmt.Printf("+ %s\n", path)
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
	} else if action == "diff" && len(params) == 2 {
		if err := diff(ctx, params[0], params[1]); err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println(usage)
	}
}
