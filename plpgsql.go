package plpgsql

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

//Open returns a connection to a postgres database.
func Open(conn string) (*sqlx.DB, error) {
	return sqlx.Connect("postgres", conn)
}

func paramSql(length int) string {
	num := make([]string, length)

	for i := 0; i < length; i++ {
		num[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf("(%s)", strings.Join(num, ","))
}

func Row(db *sqlx.DB, dest interface{}, fn string, args ...interface{}) error {
	query := fmt.Sprintf("SELECT * FROM %s%s;", fn, paramSql(len(args)))
	return db.Get(dest, query, args...)
}

func Rows(db *sqlx.DB, dest interface{}, fn string, args ...interface{}) error {
	query := fmt.Sprintf("SELECT * FROM %s%s;", fn, paramSql(len(args)))
	return db.Select(dest, query, args...)
}

func Void(db *sqlx.DB, fn string, args ...interface{}) error {
	err := Row(db, nil, fn, args...)
	return err
}

func Int64(db *sqlx.DB, fn string, args ...interface{}) (int64, error) {
	var i int64
	err := Row(db, &i, fn, args...)
	return i, err
}

func Float64(db *sqlx.DB, fn string, args ...interface{}) (float64, error) {
	var f float64
	err := Row(db, &f, fn, args...)
	return f, err
}

func String(db *sqlx.DB, fn string, args ...interface{}) (string, error) {
	var s string
	err := Row(db, &s, fn, args...)
	return s, err
}
