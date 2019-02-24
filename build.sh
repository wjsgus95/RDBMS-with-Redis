redisdir="$(pwd)/redis"

# build dependencies
(cd $redisdir/deps/ && make linenoise -j)
(cd $redisdir/deps/ && make lua -j)
(cd $redisdir/deps/ && make jemalloc -j)
(cd $redisdir/deps && make hiredis -j)

# build redis
(cd $redisdir && make -j)

# test redis
(cd $redisdir && make test)
