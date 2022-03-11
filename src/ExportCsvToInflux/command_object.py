from .exporter_object import ExporterObject
from .influx_object import InfluxObject
from .__version__ import __version__
import argparse


class UserNamespace(object):
    pass


def export_csv_to_influx():
    parser = argparse.ArgumentParser(description='CSV to InfluxDB.')

    # Parse: Parse the server name, and judge the influx version
    parser.add_argument('-s', '--server', nargs='?', default='localhost:8086', const='localhost:8086',
                        help='InfluxDB Server address. Default: localhost:8086')
    user_namespace = UserNamespace()
    parser.parse_known_args(namespace=user_namespace)
    influx_object = InfluxObject(db_server_name=user_namespace.server)
    influx_version = influx_object.get_influxdb_version()
    print('Info: The influxdb version is {influx_version}'.format(influx_version=influx_version))

    # influxdb 0.x, 1.x
    parser.add_argument('-db', '--dbname',
                        required=True if influx_version.startswith('0') or influx_version.startswith('1') else False,
                        help='For 0.x, 1.x only, InfluxDB Database name.')
    parser.add_argument('-u', '--user', nargs='?', default='admin', const='admin',
                        help='For 0.x, 1.x only, InfluxDB User name.')
    parser.add_argument('-p', '--password', nargs='?', default='admin', const='admin',
                        help='For 0.x, 1.x only, InfluxDB Password.')

    # influxdb 2.x
    parser.add_argument('-http_schema', '--http_schema', nargs='?', default='http', const='http',
                        help='For 2.x only, the influxdb http schema, could be http or https. Default: http.')
    parser.add_argument('-org', '--org', nargs='?', default='my-org', const='my-org',
                        help='For 2.x only, the org. Default: my-org.')
    parser.add_argument('-bucket', '--bucket', nargs='?', default='my-bucket', const='my-bucket',
                        help='For 2.x only, the bucket. Default: my-bucket.')
    parser.add_argument('-token', '--token',
                        required=True if influx_version.startswith('2') else False,
                        help='For 2.x only, the access token')

    # Parse: Parse the others
    parser.add_argument('-c', '--csv', required=True,
                        help='Input CSV file.')
    parser.add_argument('-d', '--delimiter', nargs='?', default=',', const=',',
                        help='CSV delimiter. Default: \',\'.')
    parser.add_argument('-lt', '--lineterminator', nargs='?', default='\n', const='\n',
                        help='CSV lineterminator. Default: \'\\n\'.')
    parser.add_argument('-m', '--measurement', required=True,
                        help='Measurement name.')
    parser.add_argument('-t', '--time_column', nargs='?', default='timestamp', const='timestamp',
                        help='Timestamp column name. Default: timestamp. '
                             'If no timestamp column, '
                             'the timestamp is set to the last file modify time for whole csv rows')
    parser.add_argument('-tf', '--time_format', nargs='?', default='%Y-%m-%d %H:%M:%S', const='%Y-%m-%d %H:%M:%S',
                        help='Timestamp format. Default: \'%%Y-%%m-%%d %%H:%%M:%%S\' e.g.: 1970-01-01 00:00:00')
    parser.add_argument('-tz', '--time_zone', nargs='?', default='UTC', const='UTC',
                        help='Timezone of supplied data. Default: UTC')
    parser.add_argument('-fc', '--field_columns', required=True,
                        help='List of csv columns to use as fields, separated by comma')
    parser.add_argument('-tc', '--tag_columns', nargs='?', default=None, const=None,
                        help='List of csv columns to use as tags, separated by comma. Default: None')
    parser.add_argument('-b', '--batch_size', nargs='?', default=500, const=500,
                        help='Batch size when inserting data to influx. Default: 500.')
    parser.add_argument('-lslc', '--limit_string_length_columns', nargs='?',  default=None, const=None,
                        help='Limit string length columns, separated by comma. Default: None.')
    parser.add_argument('-ls', '--limit_length', nargs='?', default=20, const=20,
                        help='Limit length. Default: 20.')
    parser.add_argument('-dd', '--drop_database', nargs='?', default=False, const=False,
                        help='Drop database before inserting data. Default: False')
    parser.add_argument('-dm', '--drop_measurement', nargs='?', default=False, const=False,
                        help='Drop measurement before inserting data. Default: False')
    parser.add_argument('-mc', '--match_columns', nargs='?', default=None, const=None,
                        help='Match the data you want to get for certain columns, separated by comma. '
                             'Match Rule: All matches, then match. Default: None')
    parser.add_argument('-mbs', '--match_by_string', nargs='?', default=None, const=None,
                        help='Match by string, separated by comma. Default: None')
    parser.add_argument('-mbr', '--match_by_regex', nargs='?', default=None, const=None,
                        help='Match by regex, separated by comma. Default: None')
    parser.add_argument('-fic', '--filter_columns', nargs='?', default=None, const=None,
                        help='Filter the data you want to filter for certain columns, separated by comma. '
                             'Filter Rule: Any one matches, then match. Default: None')
    parser.add_argument('-fibs', '--filter_by_string', nargs='?', default=None, const=None,
                        help='Filter by string, separated by comma. Default: None')
    parser.add_argument('-fibr', '--filter_by_regex', nargs='?', default=None, const=None,
                        help='Filter by regex, separated by comma. Default: None')
    parser.add_argument('-ecm', '--enable_count_measurement', nargs='?', default=False, const=False,
                        help='Enable count measurement. Default: False')
    parser.add_argument('-fi', '--force_insert_even_csv_no_update', nargs='?', default=True, const=True,
                        help='Force insert data to influx, even csv no update. Default: False')
    parser.add_argument('-fsc', '--force_string_columns', nargs='?', default=None, const=None,
                        help='Force columns as string type, separated by comma. Default: None.')
    parser.add_argument('-fintc', '--force_int_columns', nargs='?', default=None, const=None,
                        help='Force columns as int type, separated by comma. Default: None.')
    parser.add_argument('-ffc', '--force_float_columns', nargs='?', default=None, const=None,
                        help='Force columns as float type, separated by comma. Default: None.')
    parser.add_argument('-uniq', '--unique', nargs='?', default=False, const=False,
                        help='Write duplicated points. Default: False.')
    parser.add_argument('-v', '--version', action="version", version=__version__)

    args = parser.parse_args(namespace=user_namespace)
    exporter = ExporterObject()
    input_data = {
        'csv_file': args.csv,
        'db_server_name': user_namespace.server,
        'db_user': args.user,
        'db_password': args.password,
        'db_name': 'None' if args.dbname is None else args.dbname,
        'db_measurement': args.measurement,
        'time_column': args.time_column,
        'time_format': args.time_format,
        'time_zone': args.time_zone,
        'field_columns': args.field_columns,
        'tag_columns': args.tag_columns,
        'batch_size': args.batch_size,
        'delimiter': args.delimiter,
        'lineterminator': args.lineterminator,
        'limit_string_length_columns': args.limit_string_length_columns,
        'limit_length': args.limit_length,
        'drop_database': args.drop_database,
        'drop_measurement': args.drop_measurement,
        'match_columns': args.match_columns,
        'match_by_string': args.match_by_string,
        'match_by_regex': args.match_by_regex,
        'filter_columns': args.filter_columns,
        'filter_by_string': args.filter_by_string,
        'filter_by_regex': args.filter_by_regex,
        'enable_count_measurement': args.enable_count_measurement,
        'force_insert_even_csv_no_update': args.force_insert_even_csv_no_update,
        'force_string_columns': args.force_string_columns,
        'force_int_columns': args.force_int_columns,
        'force_float_columns': args.force_float_columns,
        'http_schema': args.http_schema,
        'org_name': args.org,
        'bucket_name': args.bucket,
        'token': 'None' if args.token is None else args.token,
        'unique': args.unique
    }
    exporter.export_csv_to_influx(**input_data)
