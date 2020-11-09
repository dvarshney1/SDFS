#!/bin/sh

if [ -d "./test_files" ] 
then
    rm -rf test_files
fi

mkdir test_files
cd test_files

dd if=/dev/urandom of=1MB.dat bs=1048576 count=1
dd if=/dev/urandom of=10MB.dat bs=1048576 count=10
dd if=/dev/urandom of=100MB.dat bs=1048576 count=100
dd if=/dev/urandom of=500MB.dat bs=1048576 count=500
dd if=/dev/urandom of=1GB.dat bs=1048576 count=1000
dd if=/dev/urandom of=10KB.dat bs=1024 count=10
dd if=/dev/urandom of=1KB.dat bs=1024 count=1
dd if=/dev/urandom of=5KB.dat bs=1024 count=5


