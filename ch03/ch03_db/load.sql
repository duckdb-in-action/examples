COPY systems FROM 'ch03_db/systems.parquet' (FORMAT 'parquet', COMPRESSION 'ZSTD');
COPY prices FROM 'ch03_db/prices.parquet' (FORMAT 'parquet', COMPRESSION 'ZSTD');
COPY readings FROM 'ch03_db/readings.parquet' (FORMAT 'parquet', COMPRESSION 'ZSTD');
