

CREATE SEQUENCE prices_id INCREMENT BY 1 MINVALUE 10 MAXVALUE 9223372036854775807 START 34 NO CYCLE;

CREATE TABLE systems(id INTEGER PRIMARY KEY, "name" VARCHAR NOT NULL);
CREATE TABLE prices(id INTEGER PRIMARY KEY DEFAULT(nextval('prices_id')), "value" DECIMAL(5,2) NOT NULL, valid_from DATE NOT NULL, valid_until DATE, UNIQUE(valid_from));
CREATE TABLE readings(system_id INTEGER, read_on TIMESTAMP, power DECIMAL(10,3) NOT NULL DEFAULT(0), CHECK((power >= 0)), PRIMARY KEY(system_id, read_on), FOREIGN KEY (system_id) REFERENCES systems(id));

CREATE OR REPLACE VIEW v_power_per_day AS
SELECT system_id,
       date_trunc('day', read_on)        AS day,
       round(sum(power)  / 4 / 1000, 2)  AS kWh,
FROM readings
GROUP BY system_id, day;
;



