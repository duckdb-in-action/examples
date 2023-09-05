COPY systems FROM 'ch05_db/systems.parquet' (FORMAT 'parquet', COMPRESSION 'ZSTD');
COPY prices FROM 'ch05_db/prices.parquet' (FORMAT 'parquet', COMPRESSION 'ZSTD');
COPY readings FROM 'ch05_db/readings.parquet' (FORMAT 'parquet', COMPRESSION 'ZSTD');
