#!/usr/bin/env bash
set -e

image="mysql/mysql-server:5.6.29"
no_gtid_cluster="1 2 3"
with_gtid_cluster="4 5 6"

docker pull $image

for i in $no_gtid_cluster
do
    docker run --name mysql_$i -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p 330$i:3306 -d $image --log-bin=binlog --read-only=1 --server-id=$i
done

sleep 15

docker ps -a

for i in $no_gtid_cluster
do
    mysql -uroot -h127.0.0.1 -P330$i < prepare.sql
done
