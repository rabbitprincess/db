package db

import (
	"context"
	"database/sql"
)

/*
func NewConn() *DBConn {
	conn := &DBConn{}
}
*/
type DBConn struct {
	driverName string
	dsn        string
	dbName     string

	db *sql.DB
}

func (t *DBConn) Init() {
}

func (t *DBConn) Connect(_driverName, _dsn, _dbName string) (err error) {
	t.driverName = _driverName
	t.dsn = _dsn
	t.dbName = _dbName

	t.db, err = sql.Open(t.driverName, t.dsn)
	if err != nil {
		return err
	}

	err = t.db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func (t *DBConn) SetOpenConns(_openConns, _idleConns int) {
	if _openConns > 0 {
		t.db.SetMaxOpenConns(_openConns)
	}
	if _idleConns > 0 {
		t.db.SetMaxIdleConns(_idleConns)
	}
}

func (t *DBConn) TxBegin(_isoLevel sql.IsolationLevel, _readonly bool) (tx *sql.Tx, err error) {
	pt_opt := &sql.TxOptions{
		Isolation: _isoLevel,
		ReadOnly:  _readonly,
	}

	return t.db.BeginTx(context.Background(), pt_opt)
}

func (t *DBConn) TxBegin__callback(_isoLevel sql.IsolationLevel, _readonly bool, _fnCallback func(*sql.Tx) error) (err error) {
	pt_opt := &sql.TxOptions{
		Isolation: _isoLevel,
		ReadOnly:  _readonly,
	}

	tx, err := t.db.BeginTx(context.Background(), pt_opt)
	if err != nil {
		return err
	}

	err = _fnCallback(tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
