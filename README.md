# Hashes [![Build Status](https://travis-ci.org/Metalnem/hashes.svg?branch=master)](https://travis-ci.org/Metalnem/hashes) [![Go Report Card](https://goreportcard.com/badge/github.com/Metalnem/hashes)](https://goreportcard.com/report/github.com/Metalnem/hashes) [![license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](https://raw.githubusercontent.com/metalnem/hashes/master/LICENSE)

Save hashes of all files in specified directories in SQLite database, and compare two databases against each other to see what's changed. Useful if you want to have append-only directories for backup, and quickly verify that no files are ever deleted.

## Usage

```
hashes create dir1 dir2 ...
hashes diff file1 file2
```
