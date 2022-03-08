Export CSV To Influx
====================

**Export CSV To Influx**: Process CSV data, and write the data to influx db

## Support:

- Influx 0.x, 1.x
- influx 2.x

> Important Note: Influx 2.x has build-in csv write feature, it is more powerful: [https://docs.influxdata.com/influxdb/v2.1/write-data/developer-tools/csv/](https://docs.influxdata.com/influxdb/v2.1/write-data/developer-tools/csv/) 

## Install

Use the pip to install the library. Then the binary **export_csv_to_influx** is ready.

```
pip install ExportCsvToInflux
```

## Features

1. **[Highlight :star2::tada::heart_eyes:]** Allow to use binary **export_csv_to_influx** to run exporter
2. **[Highlight :star2::tada::heart_eyes:]** Allow to check dozens of csv files in a folder
3. **[Highlight :star2::tada::heart_eyes::confetti_ball::four_leaf_clover::balloon:]** Auto convert csv data to int/float/string in Influx
4. **[Highlight :star2::tada::heart_eyes:]** Allow to match or filter the data by using string or regex.
5. **[Highlight :star2::tada::heart_eyes:]** Allow to count, and generate count measurement
6. Allow to limit string length in Influx
7. Allow to judge the csv has new data or not
8. Allow to use the latest file modify time as time column
9. Auto Create database if not exist
10. Allow to drop database before inserting data
11. Allow to drop measurements before inserting data

## Command Arguments

You could use `export_csv_to_influx -h` to see the help guide.

> **Note:** 
> 1. You could pass `*` to --field_columns to match all the fields: `--field_columns=*`, `--field_columns '*'`
> 2. CSV data won't insert into influx again if no update. Use to force insert: `--force_insert_even_csv_no_update=True`, `--force_insert_even_csv_no_update True`
> 3. If some csv cells have no value, auto fill the influx db based on column data type: `int: -999`, `float: -999.0`, `string: -`

| #  | Option                                   | Mandatory              | Default           | Description                                                                                                                                                                                    |
|:--:|------------------------------------------|:----------------------:|:-----------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | `-c, --csv`                              | Yes                    |                   | CSV file path, or the folder path                                                                                                                                                              |
| 2  | `-db, --dbname`                          | For 0.x, 1.x only: Yes |                   | InfluxDB Database name                                                                                                                                                                         |
| 3  | `-u, --user`                             | For 0.x, 1.x only: No  | admin             | InfluxDB User name                                                                                                                                                                             |
| 4  | `-p, --password`                         | For 0.x, 1.x only: No  | admin             | InfluxDB Password                                                                                                                                                                              |
| 5  | `-org, --org`                            | For 2.x only: No       | my-org            | For 2.x only, my org                                                                                                                                                                           |
| 6  | `-bucket, --bucket`                      | For 2.x only: No       | my-bucket         | For 2.x only, my bucket                                                                                                                                                                        |
| 7  | `-http_schema, --http_schema`            | For 2.x only: No       | http              | For 2.x only, influxdb http schema, could be http or https                                                                                                                                     |
| 8  | `-token, --token`                        | For 2.x only: Yes      |                   | For 2.x only, n                                                                                                                                                                                |
| 9  | `-m, --measurement`                      | Yes                    |                   | Measurement name                                                                                                                                                                               |
| 10 | `-fc, --field_columns`                   | Yes                    |                   | List of csv columns to use as fields, separated by comma                                                                                                                                       |
| 11 | `-tc, --tag_columns`                     | No                     | None              | List of csv columns to use as tags, separated by comma                                                                                                                                         |
| 12 | `-d, --delimiter`                        | No                     | ,                 | CSV delimiter                                                                                                                                                                                  |
| 13 | `-lt, --lineterminator`                  | No                     | \n                | CSV lineterminator                                                                                                                                                                             |
| 14 | `-s, --server`                           | No                     | localhost:8086    | InfluxDB Server address                                                                                                                                                                        |
| 15 | `-t, --time_column`                      | No                     | timestamp         | Timestamp column name. If no timestamp column, the timestamp is set to the last file modify time for whole csv rows.  `Note: Also support the pure timestamp, like: 1517587275. Auto detected` |
| 16 | `-tf, --time_format`                     | No                     | %Y-%m-%d %H:%M:%S | Timestamp format, see more: https://strftime.org/                                                                                                                                              |
| 17 | `-tz, --time_zone`                       | No                     | UTC               | Timezone of supplied data                                                                                                                                                                      |
| 18 | `-b, --batch_size`                       | No                     | 500               | Batch size when inserting data to influx                                                                                                                                                       |
| 19 | `-lslc, --limit_string_length_columns`   | No                     | None              | Limit string length column, separated by comma                                                                                                                                                 |
| 20 | `-ls, --limit_length`                    | No                     | 20                | Limit length                                                                                                                                                                                   |
| 21 | `-dd, --drop_database`                   | Compatible with 2.x: No| False             | Drop database or bucket before inserting data                                                                                                                                                  |
| 22 | `-dm, --drop_measurement`                | No                     | False             | Drop measurement before inserting data                                                                                                                                                         |
| 23 | `-mc, --match_columns`                   | No                     | None              | Match the data you want to get for certain columns, separated by comma. Match Rule: All matches, then match                                                                                    |
| 24 | `-mbs, --match_by_string`                | No                     | None              | Match by string, separated by comma                                                                                                                                                            |
| 25 | `-mbr, --match_by_regex`                 | No                     | None              | Match by regex, separated by comma                                                                                                                                                             |
| 26 | `-fic, --filter_columns`                 | No                     | None              | Filter the data you want to filter for certain columns, separated by comma. Filter Rule: Any one filter success, the filter                                                                    |
| 27 | `-fibs, --filter_by_string`              | No                     | None              | Filter by string, separated by comma                                                                                                                                                           |
| 28 | `-fibr, --filter_by_regex`               | No                     | None              | Filter by regex, separated by comma                                                                                                                                                            |
| 29 | `-ecm, --enable_count_measurement`       | No                     | False             | Enable count measurement                                                                                                                                                                       |
| 30 | `-fi, --force_insert_even_csv_no_update` | No                     | False             | Force insert data to influx, even csv no update                                                                                                                                                |
| 31 | `-fsc, --force_string_columns`           | No                     | None              | Force columns as string type, seperated as comma                                                                                                                                               |
| 32 | `-fintc, --force_int_columns`            | No                     | None              | Force columns as int type, seperated as comma                                                                                                                                                  |
| 33 | `-ffc, --force_float_columns`            | No                     | None              | Force columns as float type, seperated as comma                                                                                                                                                |

## Programmatically

Also, we could run the exporter programmatically.

```
from ExportCsvToInflux import ExporterObject

exporter = ExporterObject()
exporter.export_csv_to_influx(...)

# You could get the export_csv_to_influx parameter details by:
print(exporter.export_csv_to_influx.__doc__)
```

## Sample

1. Here is the **demo.csv**

``` 
timestamp,url,response_time
2022-03-08 02:04:05,https://jmeter.apache.org/,1.434
2022-03-08 02:04:06,https://jmeter.apache.org/,2.434
2022-03-08 02:04:07,https://jmeter.apache.org/,1.200
2022-03-08 02:04:08,https://jmeter.apache.org/,1.675
2022-03-08 02:04:09,https://jmeter.apache.org/,2.265
2022-03-08 02:04:10,https://sample-demo.org/,1.430
2022-03-08 03:54:13,https://sample-show.org/,1.300
2022-03-07 04:06:00,https://sample-7.org/,1.289
2022-03-07 05:45:34,https://sample-8.org/,2.876
```

2. Command samples
<table style="border-collapse:collapse;border-spacing:0" class="tg">
   <thead>
      <tr>
         <th style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">#</th>
         <th style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Description</th>
         <th style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Influx 0.x, 1.x</th>
         <th style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Influx 2.x</th>
      </tr>
   </thead>
   <tbody>
      <tr>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">1</td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Write whole data into influx</td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \ <br>  --csv demo.csv \<br>  --dbname demo \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --user admin \<br>  --password admin \<br>  --force_insert_even_csv_no_update True \<br>  --server 127.0.0.1:8086<br></pre>
         </td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --org my-org \<br>  --bucket my-bucket \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --token YourToken \<br>  --force_insert_even_csv_no_update True \<br>  --server 127.0.0.1:8086<br></pre>
         </td>
      </tr>
      <tr>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">2</td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Write whole data into influx, <span style="font-weight:bold"><strong>but: drop database or bucket</strong></span></td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --dbname demo \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --user admin \<br>  --password admin \<br>  --server 127.0.0.1:8086 \<br>  --force_insert_even_csv_no_update True \<br>  --drop_database=True<br></pre>
         </td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --org my-org \<br>  --bucket my-bucket \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --token YourToken \<br>  --force_insert_even_csv_no_update True \<br>  --server 127.0.0.1:8086 \<br>  --drop_database=True<br></pre>
         </td>
      </tr>
      <tr>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">3</td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Write part of data: <span style="font-weight:bold"><strong>timestamp matches 2022-03-07 and url matches sample-\d+</strong></span></td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --dbname demo \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --user admin \<br>  --password admin \<br>  --server 127.0.0.1:8086 \<br>  --drop_database=True \<br>  --force_insert_even_csv_no_update True \<br>  --match_columns=timestamp,url \<br>  --match_by_reg='2022-03-07,sample-\d+'<br></pre>
         </td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --org my-org \<br>  --bucket my-bucket \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --token YourToken \<br>  --force_insert_even_csv_no_update True \<br>  --server 127.0.0.1:8086 \<br>  --drop_database=True \<br>  --match_columns=timestamp,url \<br>  --match_by_reg='2022-03-07,sample-\d+'<br></pre>
         </td>
      </tr>
      <tr>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">4</td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Filter part of data, and write into influx: <span style="font-weight:bold"><strong>url filters sample</strong></span></td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --dbname demo \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --user admin \<br>  --password admin \<br>  --server 127.0.0.1:8086 \<br>  --drop_database True \<br>  --force_insert_even_csv_no_update True \<br>  --filter_columns url \<br>  --filter_by_reg 'sample'<br></pre>
         </td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --org my-org \<br>  --bucket my-bucket \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --token YourToken \<br>  --force_insert_even_csv_no_update True \<br>  --server 127.0.0.1:8086 \<br>  --drop_database=True \<br>&nbsp;&nbsp;--filter_columns url \<br>  --filter_by_reg 'sample'<br></pre>
         </td>
      </tr>
      <tr>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">5</td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">Enable count measurement. A new measurement named: <span style="font-weight:bold"><strong>demo.count</strong></span> generated, with match: <span style="font-weight:bold"><strong>timestamp matches 2022-03-07 and url matches sample-\d+</strong></span></td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --dbname demo \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --user admin \<br>  --password admin \<br>  --server 127.0.0.1:8086 \<br>  --drop_database True \<br>  --force_insert_even_csv_no_update True \<br>  --match_columns timestamp,url \<br>  --match_by_reg '2022-03-07,sample-\d+' \<br>  --enable_count_measurement True<br></pre>
         </td>
         <td style="border-color:inherit;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal">
            <pre>export_csv_to_influx \<br>  --csv demo.csv \<br>  --org my-org \<br>  --bucket my-bucket \<br>  --measurement demo \<br>  --tag_columns url \<br>  --field_columns response_time \<br>  --token YourToken \<br>  --force_insert_even_csv_no_update True \<br>  --server 127.0.0.1:8086 \<br>  --drop_database=True \<br>  --match_columns=timestamp,url \<br>  --match_by_reg='2022-03-07,sample-\d+' \<br>&nbsp;&nbsp;--enable_count_measurement True<br></pre>
         </td>
      </tr>
   </tbody>
</table>

3. If enable the count measurement, the count measurement is:
    
    ```text
    select * from "demo.count"
 
    name: demo.count
    time                match_timestamp match_url total
    ----                --------------- --------- -----
    1562957134000000000 3               2         9
    ```

## Special Thanks

The lib is inspired by: [https://github.com/fabio-miranda/csv-to-influxdb](https://github.com/fabio-miranda/csv-to-influxdb)
