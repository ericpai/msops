package msops

import "testing"

func TestReadDataSet(t *testing.T) {
	data, err := readDataSet(testEndpoint1, "SELECT * from data_test.tbl_test where id = ?", 1)
	if err != nil {
		t.Errorf("Test readDataSet error: %s", err.Error())
	} else if len(data) != 1 {
		t.Errorf("Test readDataSet failed: actual rowcount %d, expected 2", len(data))
	} else {
		row := data[0]
		if len(row) != 2 {
			t.Errorf("Test readDataSet failed: actual colcount %d, expected 2", len(row))
		} else if val, exist := row["name"]; !exist {
			t.Error("Test readDataSet failed: col 'name' is not found")
		} else if actData, ok := val.(string); !ok {
			t.Error("Test readDataSet failed: col 'name' is not string")
		} else if actData != "hello" {
			t.Errorf("Test readDataSet failed: actual value of col 'name' is %s, expected hello", actData)
		}
	}
}
