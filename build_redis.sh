cd redis
ls
cd deps
ls
cd jemalloc/
make clean
make -j
ls
cd ..
ls
make jemalloc -j
cd ..
ls
make clean
make -j
ls
cd deps
ls
cd jemalloc/
ls
cd ..
make -j
ls
make jemalloc -j
cd jemalloc/
ls
make -j
./autogen.sh
cd ..
ls
make jemalloc -j
cd ..
ls
cd deps
ls
make hiredis -j
cd ..
ls
make clean
make -j
make test -j
