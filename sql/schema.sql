CREATE SCHEMA IF NOT EXISTS selectel_test;

CREATE TABLE IF NOT EXISTS selectel_test."users" (
  "id" varchar PRIMARY KEY,
  "name" varchar,
  "surname" varchar
);

CREATE TABLE IF NOT EXISTS selectel_test."services" (
  "id" int PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS selectel_test."server_configs" (
  "id" varchar PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS selectel_test."orders" (
  "id" int PRIMARY KEY,
  "user_id" varchar,
  "service_id" int,
  "server_configuration" varchar,
  "service_start_date" date,
  "service_end_date" date,
  "price" int
);

ALTER TABLE selectel_test."orders" ADD FOREIGN KEY ("user_id") 
REFERENCES selectel_test."users" ("id");

ALTER TABLE selectel_test."orders" ADD FOREIGN KEY ("service_id") 
REFERENCES selectel_test."services" ("id");

ALTER TABLE selectel_test."orders" ADD FOREIGN KEY ("server_configuration") 
REFERENCES selectel_test."server_configs" ("id");
