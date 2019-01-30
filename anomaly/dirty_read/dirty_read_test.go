package dirty_read

import (
	"database/sql"
	"db-isolations/postgres"
	"db-isolations/util"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	mysqlDSN = "root:root@tcp(localhost:3306)/anomaly_test?multiStatements=true"

	mysqlDriverName = "mysql"
)

const (
	ReadUncommited = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
	ReadCommited   = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
	RepeatableRead = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	Serializable   = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
)

func TestDirtyReadOnMysql(t *testing.T) {
	db, err := OpenMysql()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should FAIL on read uncommitted", testDirtyReadWithIsolationLevel(db, ReadUncommited))
	t.Run("Should PASS on read committed", testDirtyReadWithIsolationLevel(db, ReadCommited))
	t.Run("Should PASS on repeatable read", testDirtyReadWithIsolationLevel(db, RepeatableRead))
	t.Run("Should PASS on serializable", testDirtyReadWithIsolationLevel(db, Serializable))
}

func TestDirtyReadOnPostgres(t *testing.T) {
	db, err := postgres.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should PASS on read uncommitted", testDirtyReadWithIsolationLevel(db, ReadUncommited))
	t.Run("Should PASS on read committed", testDirtyReadWithIsolationLevel(db, ReadCommited))
	t.Run("Should PASS on repeatable read", testDirtyReadWithIsolationLevel(db, RepeatableRead))
	t.Run("Should PASS on serializable", testDirtyReadWithIsolationLevel(db, Serializable))
}

func testDirtyReadWithIsolationLevel(db *sql.DB, setIsolationLevelStatement string) func(*testing.T) {
	return util.RepeatTest(func(t *testing.T) {

		util.TruncateCounters(db)

		_, err := db.Exec("INSERT INTO counters (name, counter) VALUES ('first', 10);")
		util.PanicIfNotNil(err)

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

	assert.Equal(t, 10, actualCounter)
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}

func OpenMysql() (*sql.DB, error) {
	return sql.Open(mysqlDriverName, mysqlDSN)
}
