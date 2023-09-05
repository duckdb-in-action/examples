CREATE TABLE IF NOT EXISTS readings (
    system_id   INTEGER NOT NULL,
    read_on     TIMESTAMP NOT NULL,
    power       DECIMAL(10,3) NOT NULL
            DEFAULT 0 CHECK(power >= 0), -- <.>
    PRIMARY KEY (system_id, read_on), -- <.>
    FOREIGN KEY (system_id)
            REFERENCES systems(id) -- <.>
);
