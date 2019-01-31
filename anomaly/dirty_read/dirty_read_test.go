package dirty_read

import (
	"database/sql"
	"db-isolations/mysql"
	"db-isolations/postgres"
	"db-isolations/util"
	"db-isolations/util/db/statement"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDirtyReadOnMysql(t *testing.T) {
	db, err := mysql.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should FAIL on read uncommitted", testDirtyReadWithIsolationLevel(db, statement.ReadUncommitted))
	t.Run("Should PASS on read committed", testDirtyReadWithIsolationLevel(db, statement.ReadCommitted))
	t.Run("Should PASS on repeatable read", testDirtyReadWithIsolationLevel(db, statement.RepeatableRead))
	t.Run("Should PASS on serializable", testDirtyReadWithIsolationLevel(db, statement.Serializable))
}

func TestDirtyReadOnPostgres(t *testing.T) {
	db, err := postgres.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should PASS on read uncommitted", testDirtyReadWithIsolationLevel(db, statement.ReadUncommitted))
	t.Run("Should PASS on read committed", testDirtyReadWithIsolationLevel(db, statement.ReadCommitted))
	t.Run("Should PASS on repeatable read", testDirtyReadWithIsolationLevel(db, statement.RepeatableRead))
	t.Run("Should PASS on serializable", testDirtyReadWithIsolationLevel(db, statement.Serializable))
}

func testDirtyReadWithIsolationLevel(db *sql.DB, setIsolationLevelStatement string) func(*testing.T) {
	return util.RepeatTest(func(t *testing.T) {

		resetCounter(db)

		writeDone := make(chan bool, 1)
		readDone := make(chan bool, 1)

		go runAsync(func() { incrementAndRollback(db) }, writeDone)

		go runAsync(func() { readAndAssert(db, t, setIsolationLevelStatement) }, readDone)

		<-writeDone
		<-readDone
	})
}

func incrementAndRollback(db *sql.DB) {
	_, err := db.Exec(`
		BEGIN;
		UPDATE counters SET counter = counter + 1 WHERE name='first';
		ROLLBACK;
		`)

	if err != nil {
		panic(err)
	}
}

func readAndAssert(db *sql.DB, t *testing.T, setIsolationLevelStatement string) {
	selectSQL := fmt.Sprintf(`
	%s;
	BEGIN;
	SELECT counter FROM counters WHERE name='first';
	COMMIT;
`, setIsolationLevelStatement)

	row := db.QueryRow(selectSQL)
	actualCounter := util.ScanToInt(row)

	assert.Equal(t, 0, actualCounter)
}

func resetCounter(db *sql.DB) {
	util.TruncateCounters(db)

	_, err := db.Exec("INSERT INTO counters (name, counter) VALUES ('first', 0);")
	util.PanicIfNotNil(err)
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}
