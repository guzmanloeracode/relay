package store

import (
	"io/fs"

	"github.com/pressly/goose/v3/lock"
)

func mustSub() fs.FS {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err)
	}
	return sub
}

func mustLocker() lock.SessionLocker {
	l, err := lock.NewPostgresSessionLocker()
	if err != nil {
		panic(err)
	}
	return l
}
