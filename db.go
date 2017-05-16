package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var schema = []string{
	"pragma foreign_keys = ON;",
	"create table _database_metadata_(" +
		"name text not null unique on conflict replace" +
		", value text" +
		");",
	`create table singularity(
		singularity_id integer primary key autoincrement,
		url string
	);`,
	`create table req(
		req_id integer primary key autoincrement,
		singularity_id references singularity on delete cascade,
		request_ident string,
		instances integer,
		type string,
		state string,
		captured_at timestamp
	);`,
	`create table task(
		task_id integer primary key autoincrement,
		req_id references req on delete cascade,
		deploy_ident string,
		status string
	);`,
	`create table env(
		env_id integer primary key autoincrement,
		task_id references task on delete cascade,
		name string,
		value string
	);`,
	`create table docker_image(
		docker_image_id integer primary key autoincrement,
		task_id references task on delete cascade,
		image_name string
	);`,
}

var now = time.Now()

type database struct {
	db *sql.DB
	sync.Mutex
}

func newDB() *database {
	db, err := openDB()
	if err != nil {
		panic(err)
	}

	sqlExec(db, "pragma foreign_keys = ON;")
	err = groom(db)
	if err != nil {
		panic(err)
	}

	return &database{
		db: db,
	}
}

func (db *database) close() {
	db.db.Close()
}

func (db *database) addTask(desc *taskDesc) {
	var id int64
	var err error

	db.Lock()
	defer db.Unlock()

	sid, err := db.addSing(desc.url)

	if desc.SingularityRequestParent == nil {
		id, err = db.addReq(sid, 0, desc.SingularityTaskId.RequestId, "UNKNOWN", "UNKNOWN")
	} else {
		id, err = db.addReq(
			sid,
			desc.SingularityRequestParent.Request.Instances,
			desc.SingularityRequestParent.Request.Id,
			string(desc.SingularityRequestParent.Request.RequestType),
			string(desc.SingularityRequestParent.State),
		)
	}

	if err != nil {
		debug("error getting request: %v", err)
		return
	}

	status := "UNKNOWN"
	if desc.SingularityTaskHistoryUpdate != nil {
		status = string(desc.SingularityTaskHistoryUpdate.TaskState)
	}
	debug("insert into task (req_id, deploy_ident, status) values (%v, %v, %v)", id, desc.SingularityTaskId.DeployId, status)
	stmt, err := db.db.Exec("insert into task (req_id, deploy_ident, status) values ($1, $2, $3)",
		id, desc.SingularityTaskId.DeployId, status)

	if err != nil {
		debug("error inserting task: %v", err)
		return
	}
	id, err = stmt.LastInsertId()
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

	if desc.DockerInfo != nil {
		db.db.Exec("insert into docker_image (task_id, image_name) values ($1, $2)", id, desc.DockerInfo.Image)
		if err != nil {
			debug("error inserting task docker image (%q): %v", desc.DockerInfo.Image, err)
		}
	}
}

func (db *database) addSing(url string) (int64, error) {

	rows, err := db.db.Query("select singularity_id from singularity where url= $1", url)
	defer rows.Close()
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		debug("Found existing singularity: %q = %d", url, id)
		return id, nil
	}

	debug("No existing singularity for %q", url)
	if err := rows.Err(); err != nil {
		return 0, err
	}

	stmt, err := db.db.Exec("insert into singularity (url) values ($1)", url)
	if err != nil {
		return 0, err
	}
	debug("Created new record for request %q", url)
	return stmt.LastInsertId()
}

func (db *database) addReq(singID int64, instances int32, reqID, reqType, state string) (int64, error) {
	rows, err := db.db.Query("select req_id, captured_at from req where request_ident = $1", reqID)
	defer rows.Close()
	if err != nil {
		return 0, err
	}

	var id int64
	var captureTime time.Time
	for rows.Next() {
		if err := rows.Scan(&id, &captureTime); err != nil {
			return 0, err
		}
		debug("Found existing request: %q = %d", reqID, id)
		break
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if id != 0 {
		if now.Sub(captureTime) < (1 * time.Second) {
			return id, nil
		}

		debug("Found stale request - deleting")
		db.db.Exec("delete from req where req_id = $1", id)
	}

	debug("No existing request for %q", reqID)

	stmt, err := db.db.Exec("insert into req (singularity_id, request_ident, instances, type, state, captured_at) values ($1, $2, $3, $4, $5, $6)",
		singID, reqID, instances, reqType, state, now)
	if err != nil {
		return 0, err
	}
	debug("Created new record for request %q", reqID)
	return stmt.LastInsertId()
}

func openDB() (*sql.DB, error) {
	dbFile := filepath.Join(os.TempDir(), "cygnus.db")

	debug("Recording data to %q.", dbFile)

	return sql.Open("sqlite3", "file:"+dbFile)
}

func groom(db *sql.DB) error {
	var tgp string
	schemaFingerprint := fingerPrintSchema(schema)
	err := db.QueryRow("select value from _database_metadata_ where name = 'fingerprint';").Scan(&tgp)
	if err != nil || tgp != schemaFingerprint {
		debug("Clobbering DB: %v, %q ?= %q", err, tgp, schemaFingerprint)
		if err := clobber(db); err != nil {
			return err
		}

		for _, cmd := range schema {
			if err := sqlExec(db, cmd); err != nil {
				return fmt.Errorf("Error: %v while groom DB/create: %v", err, db)
			}
		}
		if _, err := db.Exec("insert into _database_metadata_ (name, value) values"+
			" ('fingerprint', ?),"+
			" ('created', ?);",
			schemaFingerprint, now.UTC().Format(time.UnixDate)); err != nil {
			return fmt.Errorf("While grooming DB %v: %v", db, err)
		}
	}

	return nil
}

func fingerPrintSchema(schema []string) string {
	h := sha256.New()
	for i, s := range schema {
		fmt.Fprintf(h, "%d:%s\n", i, s)
	}
	buf := &bytes.Buffer{}
	b6 := base64.NewEncoder(base64.StdEncoding, buf)
	b6.Write(h.Sum([]byte(``)))
	b6.Close()
	return buf.String()
}

func clobber(db *sql.DB) error {
	if err := sqlExec(db, "PRAGMA writable_schema = 1;"); err != nil {
		return err
	}
	if err := sqlExec(db, "delete from sqlite_master where type in ('table', 'index', 'trigger');"); err != nil {
		return err
	}
	if err := sqlExec(db, "PRAGMA writable_schema = 0;"); err != nil {
		return err
	}
	if err := sqlExec(db, "vacuum;"); err != nil {
		return err
	}
	return nil
}

func sqlExec(db *sql.DB, sql string) error {
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error: %s in SQL: %s", err, sql)
	}
	return nil
}
