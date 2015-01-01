Dedupe API
==========
![](https://travis-ci.org/datamade/dedupe-api.svg?branch=master)

Repository for Enterprise Dedupe API

### Setup

**Install OS level dependencies:** 

* [Python 2.7](https://www.python.org/download/releases/2.7/)
* [Redis](http://redis.io/)

**Install app requirements**

```bash
git clone git@github.com:datamade/dedupe-api.git
cd dedupe-api
pip install "numpy>=1.6"
pip install -r requirements.txt
```

Create a PostgreSQL database for dedupeapi. (If you aren't
  already running [PostgreSQL](http://www.postgresql.org/), we recommend
  installing version 9.3 or later.)

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


## Community
* [Dedupe Google group](https://groups.google.com/forum/?fromgroups=#!forum/open-source-deduplication)
* IRC channel, #dedupe on irc.freenode.net
