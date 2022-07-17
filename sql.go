package db

import (
	"fmt"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

const (
	DEF_delimiter    = ", "
	DEF_limitNotSet  = -1
	DEF_offsetNotSet = -1
)

func SQL_insert(_from string, _field map[string]interface{}) (sql string, value []interface{}, err error) {
	return SQL_insert_multi(_from, _field)
}

func SQL_insert_multi(_from string, _field ...map[string]interface{}) (sql string, value []interface{}, err error) {
	if len(_field) < 1 {
		return "", nil, fmt.Errorf("empty field data")
	}

	value = make([]interface{}, 0, 10)
	var fieldNames []string

	// field name 구성
	var fieldName string
	{
		for fl := range _field[0] {
			fieldName += fmt.Sprintf("%s%s", fl, DEF_delimiter)
			fieldNames = append(fieldNames, fl)
		}
		fieldName = fieldName[0 : len(fieldName)-len(DEF_delimiter)]
	}

	// value 구성
	var valAll string
	{
		for _, fieldOne := range _field {
			val := "("
			// 전처리
			if len(fieldOne) != len(fieldNames) {
				return "", nil, fmt.Errorf("field length is not same")
			}
			for _, s_field_name := range fieldNames {
				i_value, is_exist := fieldOne[s_field_name]
				if is_exist == false {
					return "", nil, fmt.Errorf("field value is not exist | field name - %s", s_field_name)
				}
				val += fmt.Sprintf("?%s", DEF_delimiter)
				value = append(value, i_value)
			}
			val = val[0 : len(val)-len(DEF_delimiter)] // (value,) 안의 마지막 , 를 제거
			val += fmt.Sprintf(")%s", DEF_delimiter)
			valAll += val
		}

		valAll = valAll[0 : len(valAll)-len(DEF_delimiter)] // (value), (value), 중 마지막 , 를 제거

	}

	sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", _from, fieldName, valAll)
	return sql, value, nil
}

func SQL_select(_fields []string, _alias map[string]string, _from string, _where string, _orderBy string, _limit int, _offset int) (sql string) {
	var sqlSelect string
	{
		lenFields := len(_fields)
		lenAlias := len(_alias)

		if lenFields == 0 && lenAlias == 0 { // both emtpty
			sqlSelect = "*"
		} else {
			if lenFields != 0 { // array 중심
				for _, fieldName := range _fields {
					// [field name]
					sqlSelect += fieldName

					// [as 'alias']
					if _alias != nil { // map 정보가 있는 경우만
						s_alias_name, is_exist := _alias[fieldName]
						if is_exist == true {
							sqlSelect += fmt.Sprintf(" AS %s", s_alias_name)
						}
					}

					// [, ]
					sqlSelect += DEF_delimiter
				}
			} else if lenFields == 0 && lenAlias != 0 { // only map
				for fieldName, aliasName := range _alias {
					// [field name]
					sqlSelect += fieldName
					// [as 'alias']
					if aliasName != "" {
						sqlSelect += fmt.Sprintf(" AS %s", aliasName)
					}
					// [, ]
					sqlSelect += DEF_delimiter
				}

			}
			// * 이 아닐 경우 마지막 delimiter 를 제거한다.
			sqlSelect = sqlSelect[0 : len(sqlSelect)-len(DEF_delimiter)]
		}
	}

	if _where != "" {
		_where = fmt.Sprintf("WHERE %s", _where)
	}

	if _orderBy != "" {
		_orderBy = fmt.Sprintf("ORDER BY %s", _orderBy)
	}

	var sqlLimit string
	if _limit != DEF_limitNotSet {
		sqlLimit = fmt.Sprintf("limit %d", _limit)
	}

	var sqlOffset string
	if _offset != DEF_offsetNotSet {
		sqlOffset = fmt.Sprintf("offset %d", _offset)
	}

	sql = fmt.Sprintf("SELECT %s FROM %s %s %s %s %s;", sqlSelect, _from, _where, _orderBy, sqlLimit, sqlOffset)
	return sql
}

func SQL_update(_from string, _fields map[string]interface{}, _where string) (sql string, value []interface{}, err error) {
	value = make([]interface{}, 0, 10)

	var sqlSet string
	{
		if len(_fields) != 0 {
			for fieldName, fieldValue := range _fields {
				sqlSet += fmt.Sprintf("%s = ?%s", fieldName, DEF_delimiter)
				value = append(value, fieldValue)
			}
			// 마지막 delimiter 를 제거한다.
			sqlSet = sqlSet[0 : len(sqlSet)-len(DEF_delimiter)]
		}
	}
	sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", _from, sqlSet, _where)
	return sql, value, nil
}

func SQL_delete(_from string, _where string) (sql string) {
	sql = fmt.Sprintf("DELETE FROM %s WHERE %s;", _from, _where)
	return sql
}

func SQL_insertOnDuplicateUpdate(_from string, _fields map[string]interface{}) (sql string, value []interface{}, err error) {
	value = make([]interface{}, 0, 10)

	var sqlFields, sqlValues, sqlSet string
	{
		if len(_fields) != 0 {
			for fieldName, fieldVal := range _fields {
				sqlFields += fmt.Sprintf("%s%s", fieldName, DEF_delimiter)
				sqlValues += fmt.Sprintf("?%s", DEF_delimiter)
				sqlSet += fmt.Sprintf("%s = ?%s", fieldName, DEF_delimiter)
				value = append(value, fieldVal)
			}
			// 마지막 delimiter 를 제거한다.
			sqlFields = sqlFields[0 : len(sqlFields)-len(DEF_delimiter)]
			sqlValues = sqlValues[0 : len(sqlValues)-len(DEF_delimiter)]
			sqlSet = sqlSet[0 : len(sqlSet)-len(DEF_delimiter)]
		}
	}
	value = append(value, value...)
	sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", _from, sqlFields, sqlValues, sqlSet)
	return sql, value, nil
}

func SQL_update_RemoveNullField(_sqlUpdate string, _sets []string) (sqlUpdate_modified string, err error) {
	stmt, err := sqlparser.Parse(_sqlUpdate)
	if err != nil {
		return "", err
	}

	update, isOk := stmt.(*sqlparser.Update)
	if isOk == false {
		return "", fmt.Errorf("invalid sql | sql - %v", _sqlUpdate)
	}
	setsRemoved := make([]*sqlparser.UpdateExpr, 0, len(update.Exprs)-len(_sets))
	for _, expr := range update.Exprs {
		setName := expr.Name.Name.String()
		var isNull bool
		for _, set := range _sets {
			if setName == set {
				isNull = true
				break
			}
		}

		if isNull == false {
			setsRemoved = append(setsRemoved, expr)
		}
	}
	update.Exprs = setsRemoved

	// 후처리
	visit := sqlparser.Visit(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch data := node.(type) {
		case *sqlparser.SQLVal:
			{
				// :v1 :v2 :v3 을 ? 로 변경
				if data.Type == sqlparser.ValArg {
					data.Val = []byte("?")
					return false, nil
				}
			}
		}
		return true, nil
	})
	err = update.WalkSubtree(visit)
	if err != nil {
		return "", err
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	update.Format(buf)
	return buf.String(), nil
}
