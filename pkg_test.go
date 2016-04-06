package msops

import "testing"

const (
	testEndpoint1 = "127.0.0.1:3306"
	testEndpoint2 = "127.0.0.1:3307"
	testEndpoint3 = "127.0.0.1:3308"
	testDBAUser   = "dba"
	testDBAPass   = "dba"
	testReplUser  = "repl"
	testReplPass  = "repl"
)

var testParams = make(map[string]string)

func TestMain(m *testing.M) {
	Register(testEndpoint1, testDBAUser, testDBAPass, testReplUser, testReplPass, testParams)
	Register(testEndpoint2, testDBAUser, testDBAPass, testReplUser, testReplPass, testParams)
	Register(testEndpoint3, testDBAUser, testDBAPass, testReplUser, testReplPass, testParams)
}
