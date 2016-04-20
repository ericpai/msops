package msops

// Process represents one row data of processlist.
// Based on 5.6.30-log MySQL Community Server.
//
// Field specification can be found at https://dev.mysql.com/doc/refman/5.6/en/show-processlist.html
type Process struct {
	ID      int
	User    string
	Host    string
	DB      string
	Command string
	Time    int
	State   string
	Info    string
}

// MasterStatus represents the master status of one endpoint.
// Based on 5.6.30-log MySQL Community Server.
//
// Field specification can be found at https://dev.mysql.com/doc/refman/5.6/en/show-master-status.html
type MasterStatus struct {
	File            string
	Position        int
	BinlogDoDB      string
	BinlogIgnoreDB  string
	ExecutedGtidSet string
}

// SlaveStatus represents the slave status of one endpoint.
// Based on 5.6.30-log MySQL Community Server.
//
// Field specification can be found at https://dev.mysql.com/doc/refman/5.6/en/show-slave-status.html
type SlaveStatus struct {
	SlaveIOState              string
	MasterHost                string
	MasterUser                string
	MasterPort                int
	ConnectRetry              string
	MasterLogFile             string
	ReadMasterLogPos          int
	RelayLogFile              string
	RelayLogPos               int
	RelayMasterLogFile        string
	SlaveIORunning            string
	SlaveSQLRunning           string
	ReplicateDoDB             string
	ReplicateIgnoreDB         string
	ReplicateDoTable          string
	ReplicateIgnoreTable      string
	ReplicateWildDoTable      string
	ReplicateWildIgnoreTable  string
	LastErrno                 int
	LastError                 string
	SkipCounter               int
	ExecMasterLogPos          int
	RelayLogSpace             int
	UntilCondition            string
	UntilLogFile              string
	UntilLogPos               int
	MasterSSLAllowed          string
	MasterSSLCAFile           string
	MasterSSLCAPath           string
	MasterSSLCert             string
	MasterSSLCipher           string
	MasterSSLKey              string
	SecondsBehindMaster       int
	MasterSSLVerifyServerCert string
	LastIOErrno               int
	LastIOError               string
	LastSQLErrno              int
	LastSQLError              string
	ReplicateIgnoreServerIds  string
	MasterServerID            int
	MasterUUID                string
	MasterInfoFile            string
	SQLDelay                  int
	SQLRemainingDelay         string
	SlaveSQLRunningState      string
	MasterRetryCount          int
	MasterBind                string
	LastIOErrorTimestamp      string
	LastSQLErrorTimestamp     string
	MasterSSLCrl              string
	MasterSSLCrlpath          string
	RetrievedGtidSet          string
	ExecutedGtidSet           string
	AutoPosition              bool
}

// InnoDBStatus represents the innodb engine status of one endpoint.
// Based on 5.6.30-log MySQL Community Server.
//
// Field specification can be found at https://dev.mysql.com/doc/refman/5.6/en/innodb-standard-monitor.html
type InnoDBStatus struct {
	InnodbMutexSpinWaits  int
	InnodbMutexSpinRounds int
	InnodbMutexOSWaits    int
}
