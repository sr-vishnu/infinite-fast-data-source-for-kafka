#! /bin/bash

docker container stop $(docker container ls | grep $1 | sed -E  s'/ +/ /g' | grep -Eio '[^ ]+$')
docker container rm $(docker container ls -a | grep $1 | sed -E  s'/ +/ /g' | grep -Eio '[^ ]+$')