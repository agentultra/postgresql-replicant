#!/usr/bin/env bash

SQL="insert into replicant_profile (name) values ('testing');"

CREATE_TABLE="create table replicant_profile ( id serial primary key, name text );"

if [[ $2 = "create" ]]
then
    psql -c "$CREATE_TABLE" $1
else
    while :
    do
        psql -c "$SQL" $1
    done
fi
