# msops
[![Build Status](https://travis-ci.org/ericpai/msops.svg?branch=master)](https://travis-ci.org/ericpai/msops) [![codecov.io](https://codecov.io/github/ericpai/msops/coverage.svg?branch=master)](https://codecov.io/github/ericpai/msops?branch=master)
[![MIT license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://opensource.org/licenses/MIT)

A Go library for MySQL Ops. Based on [go-sql-driver](https://github.com/go-sql-driver/mysql).

## Requirements
go-sql-driver should be pre-installed
```bash
go get github.com/go-sql-driver/mysql
```

## Installation
```bash
go get github.com/ericpai/msops
```

## Examples

```go
package main

import (
	"fmt"

	"github.com/ericpai/msops"
)

func main() {
	dbaUsers := []string{"dba", "dba"}
	dbaPassword := []string{"password", "password"}
	replUsers := []string{"repl", "repl"}
	replPassword := []string{"password", "password"}
	dbEndpoints := []string{"127.0.0.1:3306", "127.0.0.1:3307"}
	params := map[string]string{
		"charset": "utf8",
		"timeout": "1s",
	}

	// Register DB instances
	for i := 0; i < 2; i++ {
		if err := msops.Register(dbEndpoints[i], dbaUsers[i], dbaPassword[i], replUsers[i], replPassword[i], params); err != nil {
			fmt.Printf("Register db[%d] error: %s\n", i, err.Error())
		} else {
			defer msops.Unregister(dbEndpoints[i])
		}
	}

	// Get master status of DB0
	if masterSt, err := msops.GetMasterStatus(dbEndpoints[0]); err != nil {
		fmt.Printf("Get master status of db[0] error: %s\n", err.Error())
	} else {
		fmt.Printf("Master log file: %s, log position: %d\n", masterSt.File, masterSt.Position)
	}

	// Change DB0 to be the master of DB1
	if err := msops.ChangeMasterTo(dbEndpoints[1], dbEndpoints[0], false); err != nil {
		fmt.Printf("Change master error: %s\n", err.Error())
	}

}
```


## User Guide
See API documentations [here](https://godoc.org/github.com/ericpai/msops).

## Developer Guide

If you have found bugs or want to propose some new features, please submit an issue to this repo. Pull requests are welcome as well.

## Licensing
Msops is released under [MIT](https://github.com/ericpai/msops/blob/master/LICENSE) license.
