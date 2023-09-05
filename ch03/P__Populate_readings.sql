INSTALL 'httpfs';
LOAD 'httpfs';

-- tag::sys10[]
INSERT INTO readings(system_id, read_on, power)
SELECT any_value(SiteId), -- <.>
       time_bucket(
           INTERVAL '15 Minutes',
           CAST("Date-Time" AS timestamp)
       ) AS read_on, -- <.>
       avg(
           CASE
               WHEN ac_power < 0 OR ac_power IS NULL THEN 0
               ELSE ac_power END) -- <.>
FROM
    read_csv_auto(
        'https://developer.nrel.gov/api/pvdaq/v3/' ||
        'data_file?api_key=DEMO_KEY&system_id=10&year=2019'
    )
GROUP BY read_on
ORDER BY read_on;
-- end::sys10[]

INSERT INTO readings(system_id, read_on, power)
SELECT any_value(SiteId),
       time_bucket(INTERVAL '15 Minutes', CAST("Date-Time" AS timestamp)) AS read_on,
       avg(CASE WHEN ac_power < 0 OR ac_power IS NULL THEN 0 ELSE ac_power END)
FROM read_csv_auto('https://developer.nrel.gov/api/pvdaq/v3/data_file?api_key=DEMO_KEY&system_id=10&year=2020')
GROUP BY read_on
ORDER BY read_on;

INSERT INTO readings(system_id, read_on, power)
SELECT any_value(SiteId),
       time_bucket(INTERVAL '15 Minutes', CAST("Date-Time" AS timestamp)) AS read_on,
       avg(CASE WHEN ac_power < 0 OR ac_power IS NULL THEN 0 ELSE ac_power END)
FROM read_csv_auto('https://developer.nrel.gov/api/pvdaq/v3/data_file?api_key=DEMO_KEY&system_id=1200&year=2019')
GROUP BY read_on
ORDER BY read_on;

INSERT INTO readings(system_id, read_on, power)
SELECT any_value(SiteId),
       time_bucket(INTERVAL '15 Minutes', CAST("Date-Time" AS timestamp)) AS read_on,
       avg(CASE WHEN ac_power < 0 OR ac_power IS NULL THEN 0 ELSE ac_power END)
FROM read_csv_auto('https://developer.nrel.gov/api/pvdaq/v3/data_file?api_key=DEMO_KEY&system_id=1200&year=2020')
GROUP BY read_on
ORDER BY read_on;
