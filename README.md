Dedupe API
==========

[![Build
Status](https://travis-ci.org/datamade/dedupe-api.svg?branch=master)](https://travis-ci.org/datamade/dedupe-api)

[![Coverage Status](https://coveralls.io/repos/datamade/dedupe-api/badge.png?branch=master)](https://coveralls.io/r/datamade/dedupe-api?branch=master)

Repository for Enterprise Dedupe API

### Setup

**Install OS level dependencies:** 

* [Python 3.4](https://www.python.org/)
* [Redis](http://redis.io/)
* libxml2-dev
* libxslt1-dev
* libpq-dev

**Install app requirements**

```bash
git clone git@github.com:datamade/dedupe-api.git
cd dedupe-api
pip install "numpy>=1.6"
pip install -r requirements.txt
```

Create a PostgreSQL database for dedupeapi. (If you aren't
  already running [PostgreSQL](http://www.postgresql.org/), we recommend
  installing version 9.4.)

```
createdb dedupeapi
```

Create your own `app_config.py` file:

```
cp api/app_config.py.example api/app_config.py
```

You will want to change, at the minimum, the following `app_config.py` fields:

* `DB_CONN`: edit this field to reflect your PostgreSQL
  username, server hostname, port, and database name. 

* `DEFAULT_USER`: change the username, email and password on the administrator account you will use on Plenario locally.

Before running the server, [Redis](http://redis.io/).

* To start Redis locally (in the background):
```bash
redis-server &
```

Initialize the dedupe-api database: 

```bash
python init_db.py
```

Finally, run the queue and server:

```bash
nohup python run_queue.py &
python runserver.py
```

Once the server is running, navigate to http://localhost:5000/

## Running the tests

The test runner has a separate example config file which can be used to
establish a configuration for the test runner. It will attempt to either create
a test database or drop all the tables from the test database (if it already
exists) so you'll need to make sure that the user that you are using to connect
to the database has the appropriate permissions. If your database is relatively
isolated from the outside world, you should be able to add directives such as
these to your ``pg_hba.conf`` file:

```

local all all trust
host all all 127.0.0.1/32 trust

```

Once the database is configured, you should be able to run this from the root
folder:

```
$ nosetests tests
```

## Community
* [Dedupe Google group](https://groups.google.com/forum/?fromgroups=#!forum/open-source-deduplication)
* IRC channel, #dedupe on irc.freenode.net
