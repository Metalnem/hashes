package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

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

func computeHash(path string) (string, error) {
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

func computeHashes(dirs []string) <-chan fileInfo {
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

				hash, err := computeHash(path)

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

func main() {
	dirs := os.Args[1:]

	for file := range computeHashes(dirs) {
		if file.err != nil {
			log.Fatal(file.err)
		}

		fmt.Println(file.path)
	}
}
