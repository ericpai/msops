package msops

import (
	"database/sql"
	"fmt"
	"strings"
)

// Instance records the connect information
type Instance struct {
	dbaUser       string
	dbaPassword   string
	replUser      string
	replPassword  string
	connectParams map[string]string
	connection    *sql.DB
}

var connectionPool map[string]*Instance

// ReplicationStatus represents the replication status between to instance.
// The judgement is according to the result of `SHOW SLAVE STATUS` and `SHOW MASTER STATUS`.
// Comparing the binlog file and binlog positions between master and slave
type ReplicationStatus int

const (
	// ReplStatusOK implies that in the slave status of the slave instance,
	// 'Master_Host' and 'Master_Port' are the same as the master's,
	// 'Slave_SQL_Running' and 'Slave_IO_Running' are both 'yes',
	// 'Second_Behind_Master' equals to '0'.
	ReplStatusOK ReplicationStatus = iota

	// ReplStatusSyning implies that in the slave status of the slave instance,
	// 'Master_Host' and 'Master_Port' are the same as the master's,
	// 'Slave_SQL_Running' and 'Slave_IO_Running' are both 'yes',
	// 'Second_Behind_Master' is larger than '0'.
	ReplStatusSyning

	// ReplStatusPausing implies that in the slave status of the slave instance,
	// 'Master_Host' and 'Master_Port' are the same as the master's,
	// and 'Slave_SQL_Running' and 'Slave_IO_Running' are both 'no'.
	ReplStatusPausing

	// ReplStatusError implies that in the slave status of the slave instance,
	// 'Master_Host' and 'Master_Port' are the same as the master's,
	// 'Slave_SQL_Running' and 'Slave_IO_Running' are not both 'yes',
	// and 'Last_Error' is not empty
	ReplStatusError

	// ReplStatusWrongMaster implies that in the slave status of the slave instance,
	// 'Master_Host' and 'Master_Port' are not the same as the master's.
	ReplStatusWrongMaster

	// ReplStatusUnknown implies that we cann't connect to the slave instance
	ReplStatusUnknown

	driverName = "mysql"
)

// Register registers the instance of endpoint with opening the connection with user 'dbaUser', password 'dbaPassword'.
// 'replUser' and 'replPassword' are used to be established replication by other endpoints.
// 'params' are the k-v params appending to go-mysql-driver connection string.
// 'dbaUser' should have the following privileges at least: RELOAD, PROCESS, SUPER, REPLICATION CLIENT, REPLICATION SLAVE.
// 'replUser' should have the following privileges at least: PROCESS, REPLICATION SLAVE.
// 'endpoint' show have the form "host:port"
// If the final connection string generated is invalid, an error will be returned
func Register(endpoint, dbaUser, dbaPassword, replUser, replPassword string, params map[string]string) error {
	if _, exist := connectionPool[endpoint]; !exist {
		if params == nil {
			params = make(map[string]string)
		}
		params["interpolateParams"] = "true"
		paramSlice := make([]string, 0, len(params))
		for key, value := range params {
			paramSlice = append(paramSlice, fmt.Sprintf("%s=%s", key, value))
		}
		connStr := fmt.Sprintf("%s:%s@tcp(%s)/?%s", dbaUser, dbaPassword, endpoint, strings.Join(paramSlice, "&"))
		var conn *sql.DB
		var err error
		if conn, err = sql.Open(driverName, connStr); err != nil {
			return err
		}
		inst := &Instance{
			dbaUser:       dbaUser,
			dbaPassword:   dbaPassword,
			replUser:      replUser,
			replPassword:  replPassword,
			connectParams: params,
			connection:    conn,
		}
		connectionPool[endpoint] = inst
	}
	return nil
}

// Unregister deletes the information from msops's connection pool and close the connections to endpoint
func Unregister(endpoint string) {
	if inst, exist := connectionPool[endpoint]; exist {
		inst.connection.Close()
	}
	delete(connectionPool, endpoint)
}
