package data

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	rice "github.com/GeertJohan/go.rice"
	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"
	"github.com/golang-migrate/migrate/source"
	"github.com/jmoiron/sqlx"
)

type RiceBoxSource interface {
	PopulateMigrations(box *rice.Box) error
	Open(url string) (source.Driver, error)
	Close() error
	First() (version uint, err error)
	Prev(version uint) (prevVersion uint, err error)
	Next(version uint) (nextVersion uint, err error)
	ReadUp(version uint) (r io.ReadCloser, identifier string, err error)
	ReadDown(version uint) (r io.ReadCloser, identifier string, err error)
}

// TestTruncateAll truncates all table
func TestTruncateAll(t *testing.T, connectionInfo string, databaseName string) {
	db := TestConnectDB(t, connectionInfo)
	defer db.Close()

	query := fmt.Sprintf(`SELECT
		TABLE_NAME
	FROM
		information_schema.tables
	WHERE
		table_catalog = '%s'
	AND
		table_schema = 'public'
	AND
		table_name != 'schema_migrations';`, databaseName)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to get list of tables: %v", err)
	}

	tableNames := []string{}
	for rows.Next() {
		tableName := ""
		rows.Scan(&tableName)
		tableNames = append(tableNames, tableName)
	}
	_, err = db.Exec("TRUNCATE TABLE \"" + strings.Join(tableNames, "\",\"") + "\" RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatalf("failed to truncate all tables: %v", err)
	}
}

func TestMigrateUp(t *testing.T, DBConnectionString string, sourceDriver RiceBoxSource, box *rice.Box) {
	db, err := sql.Open("postgres", DBConnectionString)
	if err != nil {
		t.Fatalf("error when open postgres connection: '%s'", err)
	}

	// Setup the source driver
	//

	err = sourceDriver.PopulateMigrations(box)
	if err != nil {
		t.Fatalf("error when creating source driver: '%s'", err)
	}

	// Setup the database driver
	//
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		t.Fatalf("error when creating postgres instance: '%s'", err)
	}

	m, err := migrate.NewWithInstance(
		"go.rice", sourceDriver,
		"postgres", driver)

	if err != nil {
		t.Fatalf("error when creating database instance: '%s'", err)
	}

	if err := m.Up(); err != nil {
		if err.Error() != "no change" {
			t.Fatalf("error when migrate up: '%s'", err)
		}
	}

	defer m.Close()
}

// TestConnectDB tests connect to the db and returns the sqlx.DB object if succeeded
func TestConnectDB(t *testing.T, connectionInfo string) *sqlx.DB {
	db, err := sqlx.Open("postgres", connectionInfo)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(time.Nanosecond)
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	return db
}
