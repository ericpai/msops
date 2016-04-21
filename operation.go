package msops

import (
	"database/sql"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
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

// StartSlave executes "START SLAVE" at the endpoint.
func StartSlave(endpoint string) error {
	var slaveInst *Instance
	var exists bool
	if slaveInst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	_, err := slaveInst.connection.Exec("START SLAVE")
	return err
}

// StopSlave executes "STOP SLAVE" at the endpoint.
func StopSlave(endpoint string) error {
	var slaveInst *Instance
	var exists bool
	if slaveInst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	_, err := slaveInst.connection.Exec("STOP SLAVE")
	return err
}

// ChangeMasterTo makes slaveEndpoint as a slave of masterEndpoint from now on.
// Use MASTER_AUTO_POSITION=1 instead of specifying the binlog file and position if useGTID is true.
func ChangeMasterTo(slaveEndpoint, masterEndpoint string, useGTID bool) error {
	var slaveInst, masterInst *Instance
	var exists bool
	var host, portStr string
	var err error
	var port int
	if slaveInst, exists = connectionPool[slaveEndpoint]; !exists {
		return errNotRegistered
	}
	if masterInst, exists = connectionPool[masterEndpoint]; !exists {
		return errNotRegistered
	}
	if host, portStr, err = net.SplitHostPort(masterEndpoint); err != nil {
		return err
	}
	if port, err = strconv.Atoi(portStr); err != nil {
		return err
	}
	if useGTID {
		_, err = slaveInst.connection.Exec("CHANGE MASTER TO MASTER_HOST=?, MASTER_PORT=?, MASTER_USER=?, MASTER_PASSWORD=?, MASTER_AUTO_POSITION=1",
			host, port, masterInst.replUser, masterInst.replPassword)
	} else if masterSt, e := GetMasterStatus(masterEndpoint); e != nil {
		return e
	} else {
		_, err = slaveInst.connection.Exec("CHANGE MASTER TO MASTER_HOST=?, MASTER_PORT=?, MASTER_USER=?, MASTER_PASSWORD=?, MASTER_LOG_FILE=?, MASTER_LOG_POS=?",
			host, port, masterInst.replUser, masterInst.replPassword, masterSt.File, masterSt.Position)
	}
	return err
}

// GetInnoDBStatus executes "SHOW engine InnoDB STATUS" and returns the 'Status' field.
func GetInnoDBStatus(endpoint string) (InnoDBStatus, error) {
	var dataSet []map[string]interface{}
	var err error
	innodbStatus := InnoDBStatus{}
	if dataSet, err = readDataSet(endpoint, "SHOW engine InnoDB STATUS"); err != nil {
		return innodbStatus, err
	}

	// There's at most one row in the resultset of "SHOW SLAVE STATUS"
	if len(dataSet) == 1 {
		lines := strings.Split(getString(dataSet[0]["Status"]), "\n")
		var section string

		for _, line := range lines {
			switch {
			case match("^BACKGROUND THREAD$", line):
				section = "BACKGROUND THREAD"
				continue
			case match("^DEAD LOCK ERRORS$", line), match("^LATEST DETECTED DEADLOCK$", line):
				section = "DEAD LOCK ERRORS"
				continue
			case match("^FOREIGN KEY CONSTRAINT ERRORS$", line), match("^LATEST FOREIGN KEY ERROR$", line):
				section = "FOREIGN KEY CONSTRAINT ERRORS"
				continue
			case match("^SEMAPHORES$", line):
				section = "SEMAPHORES"
				continue
			case match("^TRANSACTIONS$", line):
				section = "TRANSACTIONS"
				continue
			case match("^FILE I/O$", line):
				section = "FILE I/O"
				continue
			case match("^INSERT BUFFER AND ADAPTIVE HASH INDEX$", line):
				section = "INSERT BUFFER AND ADAPTIVE HASH INDEX"
				continue
			case match("^LOG$", line):
				section = "LOG"
				continue
			case match("^BUFFER POOL AND MEMORY$", line):
				section = "BUFFER POOL AND MEMORY"
				continue
			case match("^ROW OPERATIONS$", line):
				section = "ROW OPERATIONS"
				continue
			}

			if section == "SEMAPHORES" {
				matches := innodbSemaphoresExp.FindStringSubmatch(line)
				if len(matches) == 4 {
					innodbStatus.InnodbMutexSpinWaits, _ = strconv.Atoi(matches[1])
					innodbStatus.InnodbMutexSpinRounds, _ = strconv.Atoi(matches[2])
					innodbStatus.InnodbMutexOSWaits, _ = strconv.Atoi(matches[3])
				}
			}
		}
	}
	return innodbStatus, nil
}

// GetSlaveStatus executes "SHOW SLAVE STATUS" and returns the resultset.
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
		result.SlaveIOState = getString(dataSet[0]["Slave_IO_State"])
		result.MasterHost = getString(dataSet[0]["Master_Host"])
		result.MasterUser = getString(dataSet[0]["Master_User"])
		result.MasterPort = getInt(dataSet[0]["Master_Port"])
		result.ConnectRetry = getString(dataSet[0]["Connect_Retry"])
		result.MasterLogFile = getString(dataSet[0]["Master_Log_File"])
		result.ReadMasterLogPos = getInt(dataSet[0]["Read_Master_Log_Pos"])
		result.RelayLogFile = getString(dataSet[0]["Relay_Log_File"])
		result.RelayLogPos = getInt(dataSet[0]["Relay_Log_Pos"])
		result.RelayMasterLogFile = getString(dataSet[0]["Relay_Master_Log_File"])
		result.SlaveIORunning = getString(dataSet[0]["Slave_IO_Running"])
		result.SlaveSQLRunning = getString(dataSet[0]["Slave_SQL_Running"])
		result.ReplicateDoDB = getString(dataSet[0]["Replicate_Do_DB"])
		result.ReplicateIgnoreDB = getString(dataSet[0]["Replicate_Ignore_DB"])
		result.ReplicateDoTable = getString(dataSet[0]["Replicate_Do_Table"])
		result.ReplicateIgnoreTable = getString(dataSet[0]["Replicate_Ignore_Table"])
		result.ReplicateWildDoTable = getString(dataSet[0]["Replicate_Wild_Do_Table"])
		result.ReplicateWildIgnoreTable = getString(dataSet[0]["Replicate_Wild_Ignore_Table"])
		result.LastErrno = getInt(dataSet[0]["Last_Errno"])
		result.LastError = getString(dataSet[0]["Last_Error"])
		result.SkipCounter = getInt(dataSet[0]["Skip_Counter"])
		result.ExecMasterLogPos = getInt(dataSet[0]["Exec_Master_Log_Pos"])
		result.RelayLogSpace = getInt(dataSet[0]["Relay_Log_Space"])
		result.UntilCondition = getString(dataSet[0]["Until_Condition"])
		result.UntilLogFile = getString(dataSet[0]["Until_Log_File"])
		result.UntilLogPos = getInt(dataSet[0]["Until_Log_Pos"])
		result.MasterSSLAllowed = getString(dataSet[0]["Master_SSL_Allowed"])
		result.MasterSSLCAFile = getString(dataSet[0]["Master_SSL_CA_File"])
		result.MasterSSLCAPath = getString(dataSet[0]["Master_SSL_CA_Path"])
		result.MasterSSLCert = getString(dataSet[0]["Master_SSL_Cert"])
		result.MasterSSLCipher = getString(dataSet[0]["Master_SSL_Cipher"])
		result.MasterSSLKey = getString(dataSet[0]["Master_SSL_Key"])
		result.SecondsBehindMaster = getInt(dataSet[0]["Seconds_Behind_Master"])
		result.MasterSSLVerifyServerCert = getString(dataSet[0]["Master_SSL_Verify_Server_Cert"])
		result.LastIOErrno = getInt(dataSet[0]["Last_IO_Errno"])
		result.LastIOError = getString(dataSet[0]["Last_IO_Error"])
		result.LastSQLErrno = getInt(dataSet[0]["Last_SQL_Errno"])
		result.LastSQLError = getString(dataSet[0]["Last_SQL_Error"])
		result.ReplicateIgnoreServerIds = getString(dataSet[0]["Replicate_Ignore_Server_Ids"])
		result.MasterServerID = getInt(dataSet[0]["Master_Server_Id"])
		result.MasterUUID = getString(dataSet[0]["Master_UUID"])
		result.MasterInfoFile = getString(dataSet[0]["Master_Info_File"])
		result.SQLDelay = getInt(dataSet[0]["SQL_Delay"])
		result.SQLRemainingDelay = getString(dataSet[0]["SQL_Remaining_Delay"])
		result.SlaveSQLRunningState = getString(dataSet[0]["Slave_SQL_Running_State"])
		result.MasterRetryCount = getInt(dataSet[0]["Master_Retry_Count"])
		result.MasterBind = getString(dataSet[0]["Master_Bind"])
		result.LastIOErrorTimestamp = getString(dataSet[0]["Last_IO_Error_Timestamp"])
		result.LastSQLErrorTimestamp = getString(dataSet[0]["Last_SQL_Error_Timestamp"])
		result.MasterSSLCrl = getString(dataSet[0]["Master_SSL_Crl"])
		result.MasterSSLCrlpath = getString(dataSet[0]["Master_SSL_Crlpath"])
		result.RetrievedGtidSet = getString(dataSet[0]["Retrieved_Gtid_Set"])
		result.ExecutedGtidSet = getString(dataSet[0]["Executed_Gtid_Set"])
		result.AutoPosition = getBool(dataSet[0]["Auto_Position"])
	}
	return result, nil
}

// GetMasterStatus executes "SHOW MASTER STATUS" and returns the resultset.
func GetMasterStatus(endpoint string) (MasterStatus, error) {
	var dataSet []map[string]interface{}
	var err error
	var result MasterStatus
	if dataSet, err = readDataSet(endpoint, "SHOW MASTER STATUS"); err == nil {
		// There should be exactly one row in the resultset of "SHOW MASTER STATUS"
		result.File = getString(dataSet[0]["File"])
		result.Position = getInt(dataSet[0]["Position"])
		result.ExecutedGtidSet = getString(dataSet[0]["Executed_Gtid_Set"])
		result.BinlogDoDB = getString(dataSet[0]["Binlog_Do_DB"])
		result.BinlogIgnoreDB = getString(dataSet[0]["Binlog_Ignore_DB"])
	}
	return result, err
}

// GetGlobalStatus executes "SHOW GLOBAL STATUS LIKE pattern" and returns the resultset.
func GetGlobalStatus(endpoint, pattern string) (map[string]string, error) {
	var dataSet []map[string]interface{}
	var err error
	if dataSet, err = readDataSet(endpoint, "SHOW GLOBAL STATUS LIKE ?", pattern); err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, row := range dataSet {
		result[getString(row["Variable_name"])] = getString(row["Value"])
	}
	return result, nil
}

// GetGlobalVariables executes "SHOW GLOBAL VARIABLES LIKE pattern" and returns the resultset.
func GetGlobalVariables(endpoint, pattern string) (map[string]string, error) {
	var dataSet []map[string]interface{}
	var err error
	if dataSet, err = readDataSet(endpoint, "SHOW GLOBAL VARIABLES LIKE ?", pattern); err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, row := range dataSet {
		result[getString(row["Variable_name"])] = getString(row["Value"])
	}
	return result, nil
}

// SetGlobalVariable executes the statement 'SET GLOBAL key=value'.
func SetGlobalVariable(endpoint, key string, value interface{}) error {
	var inst *Instance
	var exists bool
	if inst, exists = connectionPool[endpoint]; !exists {
		return errNotRegistered
	}
	if !globalKeyExp.MatchString(key) {
		return errKeyInvalid
	}
	if _, err := inst.connection.Exec(fmt.Sprintf("SET GLOBAL %s=?", key), value); err != nil {
		return err
	}
	return nil
}

// GetProcessList executes "SHOW PROCESSLIST" and returns the resultset.
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
				ID:      getInt(row["Id"]),
				User:    getString(row["User"]),
				Host:    getString(row["Host"]),
				DB:      getString(row["db"]),
				Command: getString(row["Command"]),
				Time:    getInt(row["Time"]),
				State:   getString(row["State"]),
				Info:    getString(row["Info"]),
			},
		)
	}
	return processes, nil
}

// KillProcesses kills all the connection threads except the ones of whiteUsers.
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

// readDataSet executes the query string with placeholders replaced by args and returns the dataset.
func readDataSet(endpoint, query string, args ...interface{}) ([]map[string]interface{}, error) {
	var inst *Instance
	var exists bool
	if inst, exists = connectionPool[endpoint]; !exists {
		return nil, errNotRegistered
	}
	var err error
	var result *sql.Rows
	var columnName []string
	if result, err = inst.connection.Query(query, args...); err != nil {
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

func getString(data interface{}) string {
	bytesArr := data.(*sql.RawBytes)
	return string(*bytesArr)
}

func getInt(data interface{}) int {
	res, _ := strconv.Atoi(getString(data))
	return res
}

func getBool(data interface{}) bool {
	res, _ := strconv.ParseBool(getString(data))
	return res
}

func match(pattern, s string) bool {
	matched, err := regexp.MatchString(pattern, s)
	if err != nil {
		return false
	}
	return matched
}
