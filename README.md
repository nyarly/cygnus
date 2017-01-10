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

# Future Work

Other query options and data collection.

Capture data into
e.g. a sqlite3 or boltdb file
so that further ad hoc queries can be made
without re-collecting from the server.
