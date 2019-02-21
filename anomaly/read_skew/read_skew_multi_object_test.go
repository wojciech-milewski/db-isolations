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

	t.Run("Should FAIL on read committed", testMultiObjectReadSkew(db, statement.ReadCommitted))
	t.Run("Should PASS on repeatable read", testMultiObjectReadSkew(db, statement.RepeatableRead))
	t.Run("Should PASS on serializable", testMultiObjectReadSkew(db, statement.Serializable))
}

func testMultiObjectReadSkew(db *sql.DB, setIsolationLevelStatement string) func(*testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		resetCounters(db)

		writeDone := make(chan bool, 1)
		readDone := make(chan bool, 1)

		go runAsync(func() { setBothCounters(db, 1, setIsolationLevelStatement) }, writeDone)

		go runAsync(func() { assertCountersEqual(t, db, setIsolationLevelStatement) }, readDone)

		<-writeDone
		<-readDone
	})
}

func setBothCounters(db *sql.DB, value int, setIsolationLevelStatement string) {
	_, err := db.Exec(fmt.Sprintf(`
			%s;
			BEGIN;
			UPDATE counters SET counter=%d WHERE name='first'; 
			UPDATE counters SET counter=%d WHERE name='second'; 
			COMMIT;
			`, setIsolationLevelStatement, value, value))

	util.PanicIfNotNil(err)
}

func assertCountersEqual(t *testing.T, db *sql.DB, setIsolationLevelStatement string) {
	firstCounter, secondCounter := readCounters(db, setIsolationLevelStatement)

	assert.True(
		t,
		firstCounter == secondCounter,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstCounter, secondCounter),
	)
}

//noinspection SqlNoDataSourceInspection,SqlResolve
func readCounters(db *sql.DB, setIsolationLevelStatement string) (int, int) {
	rows, err := db.Query(fmt.Sprintf(`
			%s;
			BEGIN;
			SELECT counter FROM counters WHERE name='first';
			SELECT counter FROM counters WHERE name='second';
			COMMIT;`, setIsolationLevelStatement))

	defer util.CloseOrPanic(rows)

	util.PanicIfNotNil(err)

	firstCounter := scanCounter(rows)

	if !rows.NextResultSet() {
		panic("expected next result set")
	}

	secondCounter := scanCounter(rows)
	return firstCounter, secondCounter
}
