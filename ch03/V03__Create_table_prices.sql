CREATE SEQUENCE IF NOT EXISTS prices_id
    INCREMENT BY 1 MINVALUE 10; -- <.>

CREATE TABLE IF NOT EXISTS prices (
    id          INTEGER PRIMARY KEY
                    DEFAULT(nextval('prices_id')), -- <.>
    value       DECIMAL(5,2) NOT NULL,
    valid_from  DATE NOT NULL,
    CONSTRAINT prices_uk UNIQUE (valid_from) -- <.>
);
