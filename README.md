Export CSV To Influx
====================

**Export CSV To Influx**: Process CSV data, and export the data to influx db

## Install

Use the pip to install the library. Then the binary **export_csv_to_influx** is ready.

```
pip install ExportCsvToInflux
```

## Features

1. Allow to use binary **export_csv_to_influx** to run exporter
2. Allow to check dozens of csv files in a folder
3. Auto convert csv data to int/float/string in Influx
4. Allow to limit string length in Influx
5. Allow to judge the csv has new data or not
6. Allow to use the latest file modify time as time column
7. Auto Create database if not exist
8. Allow to drop database before inserting data
9. Allow to drop measurements before inserting data
10. Allow to match or filter the data by using string or regex.
11. Allow to count, and generate count measurement

## Command Arguments

You could use `export_csv_to_influx -h` to see the help guide.

```bash
-c, --csv, Input CSV file path, or the folder path. **Mandatory**
-d, --delimiter, CSV delimiter. Default: ','. 
-lt, --lineterminator, CSV lineterminator. Default: '\n'. 
-s, --server, InfluxDB Server address. Default: localhost:8086.
-u, --user, InfluxDB User name. Default: admin
-p, --password, InfluxDB Password. Default: admin
-db, --dbname, InfluxDB Database name. **Mandatory**
-m, --measurement, Measurement name. **Mandatory**
-t, --time_column, Timestamp column name. Default: timestamp. If no timestamp column, the timestamp is set to the last file modify time for whole csv rows.
-tf, --time_format, Timestamp format. Default: '%Y-%m-%d %H:%M:%S' e.g.: 1970-01-01 00:00:00.
-tz, --time_zone, Timezone of supplied data. Default: UTC.
-fc, --field_columns, List of csv columns to use as fields, separated by comma. **Mandatory**
-tc, --tag_columns, List of csv columns to use as tags, separated by comma. **Mandatory**
-b, --batch_size, Batch size when inserting data to influx. Default: 500.
-lslc, --limit_string_length_columns, Limit string length column, separated by comma. Default: None.
-ls, --limit_length, Limit length. Default: 20.
-dd, --drop_database, Drop database before inserting data. Default: False.
-dm, --drop_measurement, Drop measurement before inserting data. Default: False.
-mc, --match_columns, Match the data you want to get for certain columns, separated by comma. Default: None.
-mbs, --match_by_string, Match by string, separated by comma. Default: None.
-mbr, --match_by_regex, Match by regex, separated by comma. Default: None.
-fic, --filter_columns, Filter the data you want to filter for certain columns, separated by comma. Default: None.
-fibs, --filter_by_string, Filter by string, separated by comma. Default: None.
-fibr, --filter_by_regex, Filter by regex, separated by comma. Default: None.
-ecm, --enable_count_measurement, Enable count measurement. Default: False.
-fi, --force_insert_even_csv_no_update, Force insert data to influx, even csv no update. Default: False.
```

> **Note 1:** You could use the library programmablly.

  ```
  from ExportCsvToInflux import ExporterObject
  
  exporter = ExporterObject()
  exporter.export_csv_to_influx(...)
  ```

> **Note 2:** CSV data won't insert into influx again if no update. Use --force_insert_even_csv_no_update=True to force insert

## Sample

Here is the **demo.csv**.

``` 
timestamp,url,response_time
2019-07-11 02:04:05,https://jmeter.apache.org/,1.434
2019-07-11 02:04:06,https://jmeter.apache.org/,2.434
2019-07-11 02:04:07,https://jmeter.apache.org/,1.200
2019-07-11 02:04:08,https://jmeter.apache.org/,1.675
2019-07-11 02:04:09,https://jmeter.apache.org/,2.265
2019-07-11 02:04:10,https://sample-demo.org/,1.430
2019-07-12 08:54:13,https://sample-show.org/,1.300
2019-07-12 14:06:00,https://sample-7.org/,1.289
2019-07-12 18:45:34,https://sample-8.org/,2.876
```

1. Command to export whole data into influx:

    ``` 
    export_csv_to_influx \
    --csv demo.csv \
    --dbname demo \
    --measurement demo \
    --tag_columns url \
    --field_columns response_time \
    --user admin \
    --password admin \
    --server 127.0.0.1:8086
    ```

2. Command to export whole data into influx, **but: drop database**

    ```
    export_csv_to_influx \
    --csv demo.csv \
    --dbname demo \
    --measurement demo \
    --tag_columns url \
    --field_columns response_time \
    --user admin \
    --password admin \
    --server 127.0.0.1:8086 \
    --drop_database=True
    ```

3. Command to export part of data: **timestamp matches 2019-07-12 and url matches sample-\d+**

    ``` 
    export_csv_to_influx \
    --csv demo.csv \
    --dbname demo \
    --measurement demo \
    --tag_columns url \
    --field_columns response_time \
    --user admin \
    --password test-automation-monitoring-2019 \
    --server 127.0.0.1:8086 \
    --drop_database=True \
    --match_columns=timestamp,url \
    --match_by_reg='2019-07-12,sample-\d+'
    ```

4. Enable count measurement. A new measurement named: **demo.count** generated

    ```
    export_csv_to_influx \
    --csv demo.csv \
    --dbname demo \
    --measurement demo \
    --tag_columns url \
    --field_columns response_time \
    --user admin \
    --password admin \
    --server 127.0.0.1:8086 \
    --drop_database True \
    --match_columns timestamp,url \
    --match_by_reg '2019-07-12,sample-\d+' \
    --force_insert_even_csv_no_update True \
    --enable_count_measurement True 
    ```

## Special Thanks

The lib is inspired by: [https://github.com/fabio-miranda/csv-to-influxdb](https://github.com/fabio-miranda/csv-to-influxdb)
