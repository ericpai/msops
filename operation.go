package msops

import (
	"database/sql"
	"net"
)

// ResetSlave executes "RESET SLAVE ALL" if resetAll is true.
// Otherwise executes "RESET SLAVE".
func ResetSlave(endpoint string, resetAll bool) error {
	var slaveInst *Instance
	var exists bool
	var err error
	if slaveInst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	if resetAll {
		_, err = slaveInst.connection.Exec("RESET SLAVE ALL")
	} else {
		_, err = slaveInst.connection.Exec("RESET SLAVE")
	}
	return err
}

// StartSlave executes "START SLAVE" at the endpoint
func StartSlave(endpoint string) error {
	var slaveInst *Instance
	var exists bool
	if slaveInst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	_, err := slaveInst.connection.Exec("START SLAVE")
	return err
}

// StopSlave executes "STOP SLAVE" at the endpoint
func StopSlave(endpoint string) error {
	var slaveInst *Instance
	var exists bool
	if slaveInst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	_, err := slaveInst.connection.Exec("STOP SLAVE")
	return err
}

// ChangeMasterTo makes slaveEndpoint as a slave of masterEndpoint from now on. Use MASTER_AUTO_POSITION=1
// instead of specifying the binlog file and position if useGTID is true.
func ChangeMasterTo(slaveEndpoint, masterEndpoint string, useGTID bool) error {
	var slaveInst, masterInst *Instance
	var exists bool
	var host, port string
	var err error
	if slaveInst, exists = connectionPool[slaveEndpoint]; !exists {
		return errNotRegistered
	}
	if masterInst, exists = connectionPool[masterEndpoint]; !exists {
		return errNotRegistered
	}
	if host, port, err = net.SplitHostPort(masterEndpoint); err != nil {
		return err
	}
	if useGTID {
		_, err = slaveInst.connection.Exec("CHANGE MASTER TO MASTER_HOST=?, MASTER_USER=?, MASTER_PASSWORD=?, MASTER_AUTO_POSITION=1",
			host, port, masterInst.replUser, masterInst.replPassword)
	} else if masterSt, e := GetMasterStatus(masterEndpoint); e != nil {
		return e
	} else {
		_, err = slaveInst.connection.Exec("CHANGE MASTER TO MASTER_HOST=?, MASTER_USER=?, MASTER_PASSWORD=?, MASTER_LOG_FILE=?, MASTER_LOG_POS=?",
			host, port, masterInst.replUser, masterInst.replPassword, masterSt.File, masterSt.Position)
	}
	return err
}

// GetInnoDBStatus executes "SHOW engine InnoDB STATUS" and returns the 'Status' field
func GetInnoDBStatus(endpoint string) (string, error) {
	var dataSet []map[string]interface{}
	var err error
	if dataSet, err = readDataSet(endpoint, "SHOW engine InnoDB STATUS"); err != nil {
		return "", err
	}
	var result string
	// There's at most one row in the resultset of "SHOW SLAVE STATUS"
	if len(dataSet) == 1 {
		result = dataSet[0]["Status"].(string)
	}
	return result, nil
}

// GetSlaveStatus executes "SHOW SLAVE STATUS" and returns the resultset
func GetSlaveStatus(endpoint string) (SlaveStatus, error) {
	var (
		dataSet []map[string]interface{}
		result  SlaveStatus
		err     error
	)
	if dataSet, err = readDataSet(endpoint, "SHOW SLAVE STATUS"); err != nil {
		return result, err
	}
	// There's at most one row in the resultset of "SHOW SLAVE STATUS"
	if len(dataSet) == 1 {
		result.SlaveIOState = dataSet[0]["Slave_IO_State"].(string)
		result.MasterHost = dataSet[0]["Master_Host"].(string)
		result.MasterUser = dataSet[0]["Master_User"].(string)
		result.MasterPort = dataSet[0]["Master_Port"].(int)
		result.ConnectRetry = dataSet[0]["Connect_Retry"].(string)
		result.MasterLogFile = dataSet[0]["Master_Log_File"].(string)
		result.ReadMasterLogPos = dataSet[0]["Read_Master_Log_Pos"].(int)
		result.RelayLogFile = dataSet[0]["Relay_Log_File"].(string)
		result.RelayLogPos = dataSet[0]["Relay_Log_Pos"].(int)
		result.RelayMasterLogFile = dataSet[0]["Relay_Master_Log_File"].(string)
		result.SlaveIORunning = dataSet[0]["Slave_IO_Running"].(string)
		result.SlaveSQLRunning = dataSet[0]["Slave_SQL_Running"].(string)
		result.ReplicateDoDB = dataSet[0]["Replicate_Do_DB"].(string)
		result.ReplicateIgnoreDB = dataSet[0]["Replicate_Ignore_DB"].(string)
		result.ReplicateDoTable = dataSet[0]["Replicate_Do_Table"].(string)
		result.ReplicateIgnoreTable = dataSet[0]["Replicate_Ignore_Table"].(string)
		result.ReplicateWildDoTable = dataSet[0]["Replicate_Wild_Do_Table"].(string)
		result.ReplicateWildIgnoreTable = dataSet[0]["Replicate_Wild_Ignore_Table"].(string)
		result.LastErrno = dataSet[0]["Last_Errno"].(int)
		result.LastError = dataSet[0]["Last_Error"].(string)
		result.SkipCounter = dataSet[0]["Skip_Counter"].(int)
		result.ExecMasterLogPos = dataSet[0]["Exec_Master_Log_Pos"].(int)
		result.RelayLogSpace = dataSet[0]["Relay_Log_Space"].(int)
		result.UntilCondition = dataSet[0]["Until_Condition"].(string)
		result.UntilLogFile = dataSet[0]["Until_Log_File"].(string)
		result.UntilLogPos = dataSet[0]["Until_Log_Pos"].(int)
		result.MasterSSLAllowed = dataSet[0]["Master_SSL_Allowed"].(string)
		result.MasterSSLCAFile = dataSet[0]["Master_SSL_CA_File"].(string)
		result.MasterSSLCAPath = dataSet[0]["Master_SSL_CA_Path"].(string)
		result.MasterSSLCert = dataSet[0]["Master_SSL_Cert"].(string)
		result.MasterSSLCipher = dataSet[0]["Master_SSL_Cipher"].(string)
		result.MasterSSLKey = dataSet[0]["Master_SSL_Key"].(string)
		result.SecondsBehindMaster = dataSet[0]["Seconds_Behind_Master"].(int)
		result.MasterSSLVerifyServerCert = dataSet[0]["Master_SSL_Verify_Server_Cert"].(string)
		result.LastIOErrno = dataSet[0]["Last_IO_Errno"].(int)
		result.LastIOError = dataSet[0]["Last_IO_Error"].(string)
		result.LastSQLErrno = dataSet[0]["Last_SQL_Errno"].(int)
		result.LastSQLError = dataSet[0]["Last_SQL_Error"].(string)
		result.ReplicateIgnoreServerIds = dataSet[0]["Replicate_Ignore_Server_Ids"].(string)
		result.MasterServerID = dataSet[0]["Master_Server_Id"].(int)
		result.MasterUUID = dataSet[0]["Master_UUID"].(string)
		result.MasterInfoFile = dataSet[0]["Master_Info_File"].(string)
		result.SQLDelay = dataSet[0]["SQL_Delay"].(int)
		result.SQLRemainingDelay = dataSet[0]["SQL_Remaining_Delay"].(string)
		result.SlaveSQLRunningState = dataSet[0]["Slave_SQL_Running_State"].(string)
		result.MasterRetryCount = dataSet[0]["Master_Retry_Count"].(int)
		result.MasterBind = dataSet[0]["Master_Bind"].(string)
		result.LastIOErrorTimestamp = dataSet[0]["Last_IO_Error_Timestamp"].(string)
		result.LastSQLErrorTimestamp = dataSet[0]["Last_SQL_Error_Timestamp"].(string)
		result.MasterSSLCrl = dataSet[0]["Master_SSL_Crl"].(string)
		result.MasterSSLCrlpath = dataSet[0]["Master_SSL_Crlpath"].(string)
		result.RetrievedGtidSet = dataSet[0]["Retrieved_Gtid_Set"].(string)
		result.ExecutedGtidSet = dataSet[0]["Executed_Gtid_Set"].(string)
		result.AutoPosition = dataSet[0]["Auto_Position"].(bool)
	}
	return result, nil
}

// GetMasterStatus executes "SHOW MASTER STATUS" and returns the resultset
func GetMasterStatus(endpoint string) (MasterStatus, error) {
	var dataSet []map[string]interface{}
	var err error
	var result MasterStatus
	if dataSet, err = readDataSet(endpoint, "SHOW MASTER STATUS"); err == nil {
		// There should be exactly one row in the resultset of "SHOW MASTER STATUS"
		result.File = dataSet[0]["File"].(string)
		result.Position = dataSet[0]["Position"].(int)
		result.ExecutedGtidSet = dataSet[0]["Executed_Gtid_Set"].(string)
		result.BinlogDoDB = dataSet[0]["Binlog_Do_DB"].(string)
		result.BinlogIgnoreDB = dataSet[0]["Binlog_Ignore_DB"].(string)
	}
	return result, err
}

// GetGlobalVariables executes "SHOW GLOBAL VARIABLES LIKE pattern" and returns the resultset
func GetGlobalVariables(endpoint, pattern string) (map[string]interface{}, error) {
	var dataSet []map[string]interface{}
	var err error
	if dataSet, err = readDataSet(endpoint, "SHOW GLOBAL VARIABLES LIKE ?", pattern); err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	for _, row := range dataSet {
		result[row["Variable_name"].(string)] = row["Value"]
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

// GetProcessList executes "SHOW PROCESSLIST" and returns the resultset
func GetProcessList(endpoint string) ([]Process, error) {
	var dataSet []map[string]interface{}
	var err error
	if dataSet, err = readDataSet(endpoint, "SHOW PROCESSLIST"); err != nil {
		return nil, err
	}
	processes := make([]Process, 0, len(dataSet))
	for _, row := range dataSet {
		processes = append(processes,
			Process{
				ID:      row["Id"].(int),
				User:    row["User"].(string),
				Host:    row["Host"].(string),
				DB:      row["db"].(string),
				Command: row["Command"].(string),
				Time:    row["Time"].(int),
				State:   row["State"].(string),
				Info:    row["Info"].(string),
			},
		)
	}
	return processes, nil
}

// KillProcesses kills all the connection threads except the user is in whiteUsers
func KillProcesses(endpoint string, whiteUsers ...string) error {
	var inst *Instance
	var exists bool
	if inst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	var processes []Process
	var err error
	if processes, err = GetProcessList(endpoint); err != nil {
		return err
	}
	var isWhiteUser bool
	for _, process := range processes {
		isWhiteUser = false
		for _, name := range whiteUsers {
			if process.User == name {
				isWhiteUser = true
				break
			}
		}
		if !isWhiteUser {
			inst.connection.Exec("KILL ?", process.ID)
		}
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
