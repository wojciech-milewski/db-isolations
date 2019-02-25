package dirty_write

import (
	"database/sql"
	"db-isolations/postgres"
	"db-isolations/util"
	"db-isolations/util/db/statement"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	ResetCountersQueryPostgres = "INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
)

func TestShouldFindReadSkewOnPostgres_SingleObject(t *testing.T) {
	db, err := postgres.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	util.TruncateCounters(db)

	t.Run("Should FAIL on read committed", testSingleObjectReadSkew(db, statement.ReadCommitted))
	t.Run("Should PASS on repeatable read", testSingleObjectReadSkew(db, statement.RepeatableRead))
	t.Run("Should PASS on serializable", testSingleObjectReadSkew(db, statement.Serializable))
}

func testSingleObjectReadSkew(db *sql.DB, isolationLevel string) func(*testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		resetCounters(db)

		writeDone := make(chan bool, 1)
		readDone := make(chan bool, 1)

		go runAsync(func() { setCounter(db, 1) }, writeDone)

		go runAsync(func() { assertConsistentValues(t, db, isolationLevel) }, readDone)
		<-writeDone

		<-readDone
	})
}

func resetCounters(db *sql.DB) {
	_, err := db.Exec(ResetCountersQueryPostgres)
	util.PanicIfNotNil(err)
}

func setCounter(db *sql.DB, value int) {
	_, err := db.Exec(fmt.Sprintf(`
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
			BEGIN;
			UPDATE counters SET counter=%d WHERE name='first'; 
			COMMIT;
			`, value))

	if err != nil {
		panic(err)
	}
}

func assertConsistentValues(t *testing.T, db *sql.DB, isolationLevel string) {
	firstValue, secondValue := readCounter(db, isolationLevel)

	assert.True(
		t,
		firstValue == secondValue,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstValue, secondValue),
	)
}

//noinspection SqlNoDataSourceInspection,SqlResolve
func readCounter(db *sql.DB, isolationLevel string) (int, int) {
	rows, err := db.Query(fmt.Sprintf(`
			%s;
			BEGIN;
			SELECT counter FROM counters WHERE name='first';
			SELECT counter FROM counters WHERE name='first';
			COMMIT;
`, isolationLevel))
	defer util.CloseOrPanic(rows)

	util.PanicIfNotNil(err)

	firstCounter := scanCounter(rows)

	if !rows.NextResultSet() {
		panic("expected next result set")
	}

	secondCounter := scanCounter(rows)
	return firstCounter, secondCounter
}

func scanCounter(rows *sql.Rows) int {
	if !rows.Next() {
		panic(errors.New("expected row"))
	}

	var counter int
	err := rows.Scan(&counter)
	util.PanicIfNotNil(err)

	if rows.Next() {
		panic(errors.New("expected only one row"))
	}

	return counter
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}
