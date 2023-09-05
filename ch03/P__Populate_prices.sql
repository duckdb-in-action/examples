-- tag::implicit_columns[]
INSERT INTO prices
VALUES (1, 11.59, '2018-12-01', '2019-01-01');
-- end::implicit_columns[]

-- tag::mitigating_conflict[]
INSERT INTO prices
VALUES (1, 11.59, '2018-12-01', '2019-01-01')
ON CONFLICT DO NOTHING;
-- end::mitigating_conflict[]

-- tag::inserting_multiple_values[]
INSERT INTO prices(value, valid_from, valid_until)
VALUES (11.47, '2019-01-01', '2019-02-01'),
       (11.35, '2019-02-01', '2019-03-01'),
       (11.23, '2019-03-01', '2019-04-01'),
       (11.11, '2019-04-01', '2019-05-01'),
       (10.95, '2019-05-01', '2019-06-01');
-- end::inserting_multiple_values[]

-- tag::inserting_data_from_other_relations[]
INSERT INTO prices(value, valid_from, valid_until)
SELECT * FROM 'prices.csv' src;
-- end::inserting_data_from_other_relations[]

-- tag::using_update[]
UPDATE prices
SET valid_until = valid_from + INTERVAL 1 MONTH -- <.>
WHERE valid_until IS NULL;
-- end::using_update[]
