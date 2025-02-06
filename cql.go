package cql

import (
	"fmt"

	"github.com/gocql/gocql"
	// "k8s.io/apimachinery/pkg/util/wait"
)

type QueryInterface interface {
	Exec(panicIfError bool) error
	// ExecWithRetry(panicIfError bool, interval, timeout int) error
	Iter() IterInterface
}

type IterInterface interface {
	Scan(...interface{}) bool
	RowData() (RowData, error)
	Close() error
}

type Query struct {
	query *gocql.Query
}

func (q *Query) Iter() IterInterface {
	return &Iter{q.query.Iter(), q.query.Statement()}
}
func (q *Query) Exec(panicOnError bool) error {
	if err := q.Iter().Close(); err != nil {
		if panicOnError {
			panic(fmt.Sprintf("Error during query execution %s: %v", q.query.Statement(), err))
		}
		return err
	}
	return nil
}

// func (q *Query) ExecWithRetry(panicIfError bool, interval, timeout int) error {
// 	var queryError error
// 	err := wait.PollImmediate(time.Second*time.Duration(interval), time.Second*time.Duration(timeout), func() (done bool, err error) {
// 		queryError := q.Iter().Close()
// 		return queryError == nil, nil
// 	})

// 	var errorMsg string
// 	if err != nil || queryError != nil {
// 		if err != nil {
// 			errorMsg = fmt.Sprintf("All retries to execute query %s have failed", q.query.Statement())
// 		}
// 		if queryError != nil {
// 			errorMsg = fmt.Sprintf("Error during query execution %s", q.query.Statement())
// 		}

// 		if panicIfError {
// 			panic(fmt.Sprintf("Error during query execution %s: %v", q.query.Statement(), errorMsg))
// 		} else {
// 			return fmt.Errorf(errorMsg)
// 		}
// 	}
// 	return nil

// }

type RowData struct {
	RowData gocql.RowData
}

func (r *RowData) GetValue(column string) interface{} {
	for i, col := range r.RowData.Columns {
		if col == column {
			return r.RowData.Values[i]
		}
	}
	return nil
}

type Iter struct {
	iter      *gocql.Iter
	statement string
}

func (i *Iter) Scan(dest ...interface{}) bool {
	return i.iter.Scan(dest...)
}

func (i *Iter) RowData() (RowData, error) {
	result, err := i.iter.RowData()
	if err != nil {
		return RowData{}, fmt.Errorf("Error during getting row data for statement %s, err: %v", i.statement, err)
	}
	return RowData{RowData: result}, nil
}

func (i *Iter) Close() error {
	return i.iter.Close()
}
