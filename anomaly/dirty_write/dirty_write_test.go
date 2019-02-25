package dirty_write

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

func TestDirtyWriteOnMysql(t *testing.T) {
	db, err := mysql.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should PASS on read uncommitted", testDirtyWriteWithIsolationLevel(db, statement.ReadUncommitted))
	t.Run("Should PASS on read committed", testDirtyWriteWithIsolationLevel(db, statement.ReadCommitted))
	t.Run("Should PASS on repeatable read", testDirtyWriteWithIsolationLevel(db, statement.RepeatableRead))
	t.Run("Should PASS on serializable", testDirtyWriteWithIsolationLevel(db, statement.Serializable))
}

func TestDirtyWriteOnPostgres(t *testing.T) {
	db, err := postgres.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should PASS on read uncommitted", testDirtyWriteWithIsolationLevel(db, statement.ReadUncommitted))
	t.Run("Should PASS on read committed", testDirtyWriteWithIsolationLevel(db, statement.ReadCommitted))
	t.Run("Should PANIC on repeatable read", testDirtyWriteWithIsolationLevel(db, statement.RepeatableRead))
	t.Run("Should PANIC on serializable", testDirtyWriteWithIsolationLevel(db, statement.Serializable))
}

func testDirtyWriteWithIsolationLevel(db *sql.DB, setIsolationLevelStatement string) func(*testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		resetCounters(db)

		firstWriteDone := make(chan bool, 1)
		secondWriteDone := make(chan bool, 1)

		go runAsync(func() { setValues(db, 1, setIsolationLevelStatement) }, firstWriteDone)

		go runAsync(func() { setValues(db, 2, setIsolationLevelStatement) }, secondWriteDone)

		<-firstWriteDone
		<-secondWriteDone

		assertConsistentCounters(t, db)
	})
}

func resetCounters(db *sql.DB) {
	util.TruncateCounters(db)

	_, err := db.Exec("INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0);")
	util.PanicIfNotNil(err)
}

func setValues(db *sql.DB, value int, setIsolationLevelStatement string) {
	updateSQL := fmt.Sprintf(`
			%s;
			BEGIN;
			UPDATE counters SET counter=%d WHERE name='first'; 
			UPDATE counters SET counter=%d WHERE name='second'; 
			COMMIT;
			`, setIsolationLevelStatement, value, value)
	_, err := db.Exec(updateSQL)
	util.PanicIfNotNil(err)
}

func assertConsistentCounters(t *testing.T, db *sql.DB) {
	row := db.QueryRow(`SELECT counter FROM counters WHERE name='first';`)
	firstCounter := util.ScanToInt(row)

	row = db.QueryRow(`SELECT counter FROM counters WHERE name='second'`)
	secondCounter := util.ScanToInt(row)

	assert.True(
		t,
		firstCounter == secondCounter,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstCounter, secondCounter),
	)
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}
