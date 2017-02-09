# Cygnus

Scan a Singularity and return environment variables for running tasks.

# Usage

```
cygnus [options] [(--env=<env>)...] <singularity url>

Options:
	-H, --no-print-headers  Don't print the header prologue
	-A, --no-print-active   Do not print the active deploys
	-p, --print-pending     Also include pending deploys
	--env=<env>             Environment variables to queury
	-x <num>                Use environment default <num>
```


Environment defaults are sets of useful environment variables, collected over
time by users of the tool.

```
-x 1: TASK_HOST, PORT0
```

# Data Collection

Regardless of the environment variables queried on the command line,
Cygnus also creates a sqlite file at $TEMPDIR/cygnus.db,
which can be reviewed with `sqlite3 $TEMPDIR/cygnus.db`.
This file is deleted and replaced on every invocation.

From there, consider `.tables`
(as no guarantees are made about the schema.)
As of this moment, you might try something like:
```
⮀ sqlite3 $TMPDIR/cygnus.db
sqlite> .headers on
sqlite> .mode column
sqlite> select * from task natural join env;
task_id     request_ident             deploy_ident                      env_id      name         value
----------  ------------------------  --------------------------------  ----------  -----------  ----------
1           asdfasdfasasdfasdfasdfas  3f3c5e7602a84e64917a9dda788697e3  1           INSTANCE_NO  1
1           asdfasdfasasdfasdfasdfas  3f3c5e7602a84e64917a9dda788697e3  2           TASK_HOST    localhost
1           asdfasdfasasdfasdfasdfas  3f3c5e7602a84e64917a9dda788697e3  3           TASK_REQUES  192.168.99
```
