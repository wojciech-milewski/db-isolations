package dirty_write

import (
	"database/sql"
	"db-isolations/util"
	"db-isolations/util/db/statement"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"testing"
)

//noinspection SqlNoDataSourceInspection,SqlResolve
const (
	ResetCountersQueryPostgres      = "INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
	ResetSingleCounterQueryPostgres = "INSERT INTO counters (name, counter) VALUES ('first', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
)

func TestShouldFindReadSkewOnPostgres_MultiObject(t *testing.T) {
	db, err := OpenPostgres()
	util.PanicIfNotNil(err)
	defer util.CloseOrPanic(db)

	util.TruncateCounters(db)

	t.Run("Should FAIL on read committed", testMultiObjectReadSkew(db, statement.ReadCommitted))
	t.Run("Should PASS on repeatable read", testMultiObjectReadSkew(db, statement.RepeatableRead))
	t.Run("Should PASS on serializable", testMultiObjectReadSkew(db, statement.Serializable))
}

func testMultiObjectReadSkew(db *sql.DB, setIsolationLevelStatement string) func(*testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		_, err := db.Exec(ResetCountersQueryPostgres)
		util.PanicIfNotNil(err)

		writeDone := make(chan bool, 1)
		readDone := make(chan bool, 1)

		go runAsync(func() { setValues(db, 1, setIsolationLevelStatement) }, writeDone)

		go runAsync(func() { assertConsistentCounters(t, db, setIsolationLevelStatement) }, readDone)
		<-writeDone

		<-readDone
	})
}

func TestShouldFindReadSkewOnPostgres_SingleObject(t *testing.T) {
	db, err := OpenPostgres()
	util.PanicIfNotNil(err)
	defer util.CloseOrPanic(db)

	util.TruncateCounters(db)

	t.Run("Should FAIL on read committed", util.RepeatTest(func(t *testing.T) {
		_, err = db.Exec(ResetSingleCounterQueryPostgres)
		util.PanicIfNotNil(err)

		writeDone := make(chan bool, 1)
		readDone := make(chan bool, 1)

		go runAsync(func() { setCounter(db, 1) }, writeDone)

		go runAsync(func() { assertConsistentValues(t, db) }, readDone)
		<-writeDone

		<-readDone
	}))

	for i := 1; i <= 5000; i++ {

	}
}

func setValues(db *sql.DB, value int, setIsolationLevelStatement string) {
	_, err := db.Exec(fmt.Sprintf(`
			%s;
			BEGIN;
			UPDATE counters SET counter=%d WHERE name='first'; 
			UPDATE counters SET counter=%d WHERE name='second'; 
			COMMIT;
			`, setIsolationLevelStatement, value, value))

	if err != nil {
		panic(err)
	}
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

func assertConsistentCounters(t *testing.T, db *sql.DB, setIsolationLevelStatement string) {
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

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	if err != nil {
		panic(err)
	}

	firstCounter := scanCounter(rows)

	if !rows.NextResultSet() {
		panic("expected next result set")
	}

	secondCounter := scanCounter(rows)
	return firstCounter, secondCounter
}

func assertConsistentValues(t *testing.T, db *sql.DB) {
	firstValue, secondValue := readCounter(db)

	assert.True(
		t,
		firstValue == secondValue,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstValue, secondValue),
	)
}

//noinspection SqlNoDataSourceInspection,SqlResolve
func readCounter(db *sql.DB) (int, int) {
	rows, err := db.Query(`
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
			SELECT counter FROM counters WHERE name='first';
			SELECT counter FROM counters WHERE name='first';
COMMIT;
`)

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	if err != nil {
		panic(err)
	}

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
	if err != nil {
		panic(err)
	}

	if rows.Next() {
		panic(errors.New("expected only one row"))
	}

	return counter
}

const (
	postgresDSN        = "postgres://root:root@localhost:5432/anomaly_test?sslmode=disable"
	postgresDriverName = "postgres"
)

func OpenPostgres() (*sql.DB, error) {
	return sql.Open(postgresDriverName, postgresDSN)
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}
