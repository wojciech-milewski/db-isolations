package dirty_write

import (
	"database/sql"
	"db-isolations/postgres"
	"db-isolations/util"
	"db-isolations/util/db/statement"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShouldFindReadSkewOnPostgres_MultiObject(t *testing.T) {
	db, err := postgres.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	util.TruncateCounters(db)

	t.Run("Should FAIL on read committed", testMultiObjectReadSkew(db, statement.ReadCommitted, sql.LevelReadCommitted))
	t.Run("Should PASS on repeatable read", testMultiObjectReadSkew(db, statement.RepeatableRead, sql.LevelRepeatableRead))
	t.Run("Should PASS on serializable", testMultiObjectReadSkew(db, statement.Serializable, sql.LevelSerializable))
}

func testMultiObjectReadSkew(db *sql.DB, setIsolationLevelStatement string, isolationLevel sql.IsolationLevel) func(*testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		resetCounters(db)

		writeDone := make(chan bool, 1)
		readDone := make(chan bool, 1)

		go runAsync(func() { setBothCounters(db, 1, setIsolationLevelStatement, isolationLevel) }, writeDone)

		go runAsync(func() { assertCountersEqual(t, db, isolationLevel) }, readDone)

		<-writeDone
		<-readDone
	})
}

func setBothCounters(db *sql.DB, value int, setIsolationLevelStatement string, isolationLevel sql.IsolationLevel) {
	_, err := db.Exec(fmt.Sprintf(`
			%s;
			BEGIN;
			UPDATE counters SET counter=%d WHERE name='first'; 
			UPDATE counters SET counter=%d WHERE name='second'; 
			COMMIT;
			`, setIsolationLevelStatement, value, value))

	util.PanicIfNotNil(err)
}

func assertCountersEqual(t *testing.T, db *sql.DB, isolationLevel sql.IsolationLevel) {
	firstCounter, secondCounter := readCounters(db, isolationLevel)

	assert.True(
		t,
		firstCounter == secondCounter,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstCounter, secondCounter),
	)
}

//noinspection SqlNoDataSourceInspection,SqlResolve
func readCounters(db *sql.DB, isolationLevel sql.IsolationLevel) (int, int) {
	transaction := util.BeginTx(db, isolationLevel)

	row := transaction.QueryRow("SELECT counter FROM counters WHERE name='first';")
	firstCounter := util.ScanToInt(row)

	row = transaction.QueryRow("SELECT counter FROM counters WHERE name='second';")
	secondCounter := util.ScanToInt(row)

	err := transaction.Commit()
	util.PanicIfNotNil(err)

	return firstCounter, secondCounter
}
