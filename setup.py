from setuptools import setup, find_packages
import os
import re

CURDIR = os.path.dirname(os.path.abspath(__file__))
url = 'https://github.com/Bugazelle/export-csv-to-inlfux'

with open(os.path.join(CURDIR, 'src', 'ExportCsvToInflux', '__version__.py')) as f:
    VERSION = re.search("__version__ = '(.*)'", f.read()).group(1)
    download_url = '{0}/archive/v{1}.tar.gz'.format(url, VERSION)

long_description = '''
Export CSV To Influx
====================

**Export CSV To Influx**: Process CSV data, and export the data to influx db

**Install**

Use the pip to install the library. Then the binary **export_csv_to_influx** is ready.

```
pip install ExportCsvToInflux
```

**Features**

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

**Command Arguments**

You could use `export_csv_to_influx -h` to see the help guide.


-c, --csv, Input CSV file path, or the folder path. **Mandatory**

-d, --delimiter, CSV delimiter. Default: ','. 

-lt, --lineterminator, CSV lineterminator. Default: '\n'. 

-s, --server, InfluxDB Server address. Default: localhost:8086.

-u, --user, InfluxDB User name.

-p, --password, InfluxDB Password. 

-db, --dbname, InfluxDB Database name. **Mandatory**

-m, --measurement, Measurement name. **Mandatory**

-t, --time_column, Timestamp column name. Default: timestamp. If no timestamp column, the timestamp is set to the last file modify time for whole csv rows.

-tf, --time_format, Timestamp format. Default: '%Y-%m-%d %H:%M:%S' e.g.: 1970-01-01 00:00:00.

-tz, --time_zone, Timezone of supplied data. Default: UTC.

-fc, --field_columns, List of csv columns to use as fields, separated by comma. **Mandatory**

-tc, --tag_columns, List of csv columns to use as tags, separated by comma. **Mandatory**

-b, --batch_size, Batch size when inserting data to influx. Default: 500.

-lslc, --limit_string_length_columns, Limit string length column. Default: None.

-ls, --limit_length, Limit length. Default: 20.

-dd, --drop_database, Drop database before inserting data.

-dm, --drop_measurement, Drop measurement before inserting data.

-mc, --match_columns, Match the data you want to get for certain columns, separated by comma.

-mbs, --match_by_string, Match by string, separated by comma.

-mbr, --match_by_regex, Match by regex, separated by comma.

-fic, --filter_columns, Filter the data you want to filter for certain columns, separated by comma.

-fibs, --filter_by_string, Filter by string, separated by comma.

-fibr, --filter_by_regex, Filter by regex, separated by comma.

-ecm, --enable_count_measurement, Enable count measurement.

-fi, --force_insert_even_csv_no_update, Force insert data to influx, even csv no update.

For more info, please refer to the https://github.com/Bugazelle/export-csv-to-inlfux
'''

setup(
    name='ExportCsvToInflux',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    version=VERSION,
    license='bsd-3-clause-clear',
    description='Export',
    zip_safe=False,
    include_package_data=True,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Bugazelle',
    author_email='463407426@qq.com',
    keywords=['python', 'csv', 'influx'],
    install_requires=['influxdb', ],
    download_url=download_url,
    url=url,
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ),
    entry_points={
        'console_scripts': [
            'export_csv_to_influx = ExportCsvToInflux.exporter_object:export_csv_to_influx',
        ],
    },
)
