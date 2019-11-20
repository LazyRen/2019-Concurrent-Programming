# MariaDB Terminal Commands

Assume cwd = `.../project4`

## Execution

```shell
mariadb/server/inst/bin/mysqld --defaults-file=mariadb/server/inst/my.cnf
```

#### Run(on other terminal)

```shell
mariadb/server/inst/bin/mysql
```

#### Shutdown

```shell
mariadb/server/inst/bin/mysqladmin shutdown
```



## Making & Uploading *patchfile* to git

```shell
cd mariadb/server
git diff --no-prefix > patchfile            # making a patch file
cd ../..
mv mariadb/server/patchfile .               # move the file to project4 directory
git add .                                   # mariadb directory must be ignored
git commit -m “...”
git push
```



## Sysbench (OLTP Benchmark)

#### Creating a Test Database

Before running OLTP test, you should create a test database.

```shell
mariadb/server/inst/bin/mysqladmin create sbtest   # create a test database
```

#### Preparing a Database Table

Prepare a database table in which queries will be executed.
**MariaDB server need to be running**

```shell
sysbench --db-driver=mysql --mysql-host=localhost --report-interval=1 --mysql-port=12943 --mysql-socket=./mariadb/server/inst/mysql.sock --mysql-user=$USER --mysql-db=sbtest --threads=10 --table-size=10000 --tables=1 --time=60 /usr/share/sysbench/oltp_read_write.lua prepare
```

#### Execution

Execute queries to profile a database performance.

```shell
sysbench --db-driver=mysql --mysql-host=localhost --report-interval=1 --mysql-port=12943 --mysql-socket=./mariadb/server/inst/mysql.sock --mysql-user=$USER --mysql-db=sbtest --threads=10 --table-size=10000 --tables=1 --time=60 /usr/share/sysbench/oltp_read_write.lua run
```

#### Changing Parameters

Drop and re-create the test database(sbtest) before changing some sysbench parameters related to the test data.

```shell
mariadb/server/inst/bin/mysqladmin drop sbtest

mariadb/server/inst/bin/mysqladmin create sbtest

# Repeat prepare & execution
```
