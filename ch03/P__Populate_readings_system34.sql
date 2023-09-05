INSTALL 'httpfs';
LOAD 'httpfs';

-- tag::year2019[]
INSERT INTO readings(system_id, read_on, power)
SELECT SiteId, "Date-Time",
       CASE
           WHEN ac_power < 0 OR ac_power IS NULL THEN 0
           ELSE ac_power END
FROM read_csv_auto('https://developer.nrel.gov/api/pvdaq/v3/data_file?api_key=DEMO_KEY&system_id=34&year=2019');
-- end::year2019[]

INSERT INTO readings(system_id, read_on, power)
SELECT SiteId, "Date-Time",
       CASE
           WHEN ac_power < 0 OR ac_power IS NULL THEN 0
           ELSE ac_power END
FROM read_csv_auto('https://developer.nrel.gov/api/pvdaq/v3/data_file?api_key=DEMO_KEY&system_id=34&year=2020');

-- tag::cleanup[]
DELETE FROM readings
WHERE date_part('minute', read_on) NOT IN (0,15,30,45);
-- end::cleanup[]
