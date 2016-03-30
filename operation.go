package msops

import "database/sql"

// GetGlobalVariables executes "SHOW GLOBAL VARIABLES LIKE pattern" and returns the resultset
func GetGlobalVariables(endpoint, pattern string) (map[string]string, error) {
	var dataSet []map[string]interface{}
	var err error
	if dataSet, err = readDataSet(endpoint, "SHOW GLOBAL VARIABLES LIKE ?", pattern); err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, row := range dataSet {
		result[row["Variable_name"].(string)] = row["Value"].(string)
	}
	return result, nil
}

// SetGlobalVariable executes the statement 'SET GLOBAL key=value'
func SetGlobalVariable(endpoint, key string, value interface{}) error {
	var inst *Instance
	var exists bool
	if inst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	if _, err := inst.connection.Exec("SET GLOBAL ?=?", key, value); err != nil {
		return err
	}
	return nil
}

// readDataSet executes the query string with placeholders replaced by args and returns the dataset
func readDataSet(endpoint, query string, args ...interface{}) ([]map[string]interface{}, error) {
	var inst *Instance
	var exists bool
	if inst, exists = connectionPool[endpoint]; !exists {
		return nil, errNotRegistered
	}
	var err error
	var result *sql.Rows
	var columnName []string
	if result, err = inst.connection.Query(query, args); err != nil {
		return nil, err
	}
	defer result.Close()
	if columnName, err = result.Columns(); err != nil {
		return nil, err
	}
	columnCount := len(columnName)
	columnValue := make([]interface{}, columnCount)
	var dataset []map[string]interface{}
	for result.Next() {
		for i := 0; i < columnCount; i++ {
			columnValue[i] = new(sql.RawBytes)
		}
		if err = result.Scan(columnValue...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i := 0; i < columnCount; i++ {
			row[columnName[i]] = columnValue[i]
		}
		dataset = append(dataset, row)
	}
	return dataset, nil
}
