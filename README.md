## RDBMS Using with Redis

User Interface in python tokenizes and parses given SQL queries and executes them on redis cluster.

Supports:

create, insert, select, delete and update commands on single table.(cartesian product not supported)

select with which, group by, having clause using aggregation and regular expression.

### Build Redis

./build.sh

### Dependencies

redis dependencies are built using build.sh,

python modules are listed in Pipfile. Use pipenv shell.


### Usage

python main.py

and when you see a message like this:

'''
Starting 6379

Starting 6380

Starting 6381

\>\>\> Performing hash slots allocation on 3 nodes...

Master[0] -> Slots 0 - 5460

Master[1] -> Slots 5461 - 10922

Master[2] -> Slots 10923 - 16383

M: 15f47cf0fbc610649c58dc558027fd23411e10fc 127.0.0.1:6379

   slots:[0-5460] (5461 slots) master

M: 6918d5cfdb8e10422155d78def1bb6a30fee123f 127.0.0.1:6380

   slots:[5461-10922] (5462 slots) master

M: f663e41d77f17fea3c416cbda73c162ac7b69a15 127.0.0.1:6381

   slots:[10923-16383] (5461 slots) master

Can I set the above configuration? (type 'yes' to accept):
'''

type yes and you're set up and good to go :)



### Architecture

In parse.py, SQL query is first parsed and encoded to a bytestring and passed onto parser we augmented to redis, namely rdbms\_utils.c.
You can check out the encoding rule in parser\_spec.txt.
Also, rdbms.c is the main routine of sql query handling which we also augmented to redis.

Redis runs in cluster with 3 master nodes, which is the main reason why we couldn't support cartesian product of more than one tables.
