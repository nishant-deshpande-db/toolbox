## File manifest for Delta Tables

### Quick start

 - Open the `use_jar_example` notebook. Copy it. Change the parameters. Run on a Databricks 16.4 LTS cluster.
 - Once finished, use the `sql for table history manifest`. Change the parameter to your new output table from (1). Run the SQL.

### Details

Every version of a Delta table is made up of parquet files. (There are also other artifacts, like binary deletion vectors, CRC files, metadata etc.)

The `TableHistoryManifest` utility dumps out the files and the file sizes for all files in some or all versions of a Delta table, by reading the delta metadata.

Note that it does not check if the files actually exist.

The utility also marks each file record with two flags: is_retention_period and is_latest.

 - is_latest: 1 if the file is in the latest version of the table, 0 otherwise.
 - is_retention_period: 1 if the file is within the `delta.deletedFileRetentionDuration` of the table. This is extracted from each table, otherwise the current default of 7 days is applied.

The full schema of the output table:

(table_name, table_location, table_version_number, versino_timestamp, file_name, file_size, in_retention_period, latest_version).

See `use_jar_example` notebook on how to use the utility.

See `sql for table history manifest` to get a SQL query on the output table that lists the tables by total size of the retained files.

Note:

 - The default configuration gets versions within the `delta.deletedFileRetentionDuration` period of each table. It is possible to get the files for all versions of the table (in the delta metadata), but for tables with a lot of versions, this may take a long time.
 - Run on a small set of tables (or a schema with a few tables) first, get familiar with the output and then run on larger sets.
 - Each version file list is a spark job. There are options on parallelism across tables, but for each table the file list fetch is serial.
