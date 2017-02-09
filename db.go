package main

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

var schema = []string{
	"pragma foreign_keys = ON;",
	`create table task(
		task_id integer primary key autoincrement,
		request_ident string,
		deploy_ident string
	);`,
	`create table env(
		env_id integer primary key autoincrement,
		task_id references task,
		name string,
		value string
	);`,
}

type database struct {
	db *sql.DB
}

func newDB() *database {
	db, err := openDB()
	if err != nil {
		panic(err)
	}

	createSchema(db)

	return &database{
		db: db,
	}
}

func (db *database) close() {
	db.db.Close()
}

func (db *database) addTask(desc *taskDesc) {
	stmt, err := db.db.Exec("insert into task (request_ident, deploy_ident) values ($1, $2)",
		desc.SingularityTaskId.RequestId, desc.SingularityTaskId.DeployId)
	if err != nil {
		debug("error inserting task: %v", err)
		return
	}
	id, err := stmt.LastInsertId()
	if err != nil {
		debug("error getting new task db id: %v", err)
		return
	}

	for _, vrb := range desc.Env().Variables {
		db.db.Exec("insert into env (task_id, name, value) values ($1, $2, $3)", id, vrb.Name, vrb.Value)
		if err != nil {
			debug("error inserting task env pair (%q: %q): %v", vrb.Name, vrb.Value, err)
		}
	}
}

func openDB() (*sql.DB, error) {
	dbFile := filepath.Join(os.TempDir(), "cygnus.db")

	debug("Recording data to %q.", dbFile)
	os.Remove(dbFile)

	return sql.Open("sqlite3", "file:"+dbFile)
}

func createSchema(db *sql.DB) {
	for _, cmd := range schema {
		if _, err := db.Exec(cmd); err != nil {
			panic(err)
		}
	}
}
