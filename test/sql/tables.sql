

DROP SCHEMA IF EXISTS related_db_cluster CASCADE;
CREATE SCHEMA related_db_cluster;

CREATE TABLE related_db_cluster."testTable" (id serial NOT NULL, CONSTRAINT "pk_testTable" PRIMARY KEY (id));