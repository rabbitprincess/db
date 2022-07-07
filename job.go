package db

import "database/sql"

// db or tx
type DBJob struct {
	isTx bool

	db *sql.DB
	tx *sql.Tx
}

func (t *DBJob) Init(_is_tx bool, _i_db *sql.DB, _i_tx *sql.Tx) {
	t.isTx = _is_tx
	t.db = _i_db
	t.tx = _i_tx
}

// args 제작 예정
func (t *DBJob) Exec(_sql string, _args ...interface{}) (res sql.Result, err error) {
	if t.isTx == false {
		res, err = t.db.Exec(_sql, _args...)
	} else {
		res, err = t.tx.Exec(_sql, _args...)
	}
	return res, err
}

func (t *DBJob) Query(_sql string, _args ...interface{}) (rows *sql.Rows, err error) {
	if t.isTx == false {
		rows, err = t.db.Query(_sql, _args...)
	} else {
		rows, err = t.tx.Query(_sql, _args...)
	}
	return rows, err
}
