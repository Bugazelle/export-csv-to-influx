from pytz.exceptions import UnknownTimeZoneError
from .influx_object import InfluxObject
from collections import defaultdict
from .base_object import BaseObject
from .__version__ import __version__
from .csv_object import CSVObject
from pytz import timezone
import collections
import argparse
import datetime
import uuid
import csv
import sys
import os
import re

try:
    from influxdb_client import WriteOptions
except ImportError:
    pass


class ExporterObject(object):
    """ExporterObject"""

    def __init__(self):
        self.match_count = defaultdict(int)
        self.filter_count = defaultdict(int)
        self._write_response = None

    def __error_cb(self, details, data, exception):
        """Private Function: error callback for write api"""
        self._write_response = False

    @staticmethod
    def __process_tags_fields(columns,
                              row,
                              limit_length,
                              int_type,
                              float_type,
                              limit_string_length_columns,
                              force_string_columns,
                              force_int_columns,
                              force_float_columns,
                              unique):
        """Private function: __process_tags_fields"""

        results = dict()
        for column in columns:
            v = 0
            if column in row:
                v = row[column]
                if limit_string_length_columns and column in limit_string_length_columns:
                    v = str(v)[:limit_length + 1]
                if force_string_columns and column in force_string_columns:
                    v = str(v)
                if force_int_columns and column in force_int_columns:
                    try:
                        v = int(v)
                    except ValueError:
                        print('Warning: Failed to force "{0}" to int, skip...'.format(v))
                if force_float_columns and column in force_float_columns:
                    try:
                        v = float(v)
                    except ValueError:
                        print('Warning: Failed to force "{0}" to float, skip...'.format(v))

                # If field is empty
                if len(str(v)) == 0:
                    # Process the empty
                    if int_type[column] is True:
                        v = -999
                    elif float_type[column] is True:
                        v = -999.0
                    else:
                        v = '-'

                    # Process the force
                    if force_string_columns and column in force_string_columns:
                        v = '-'
                    if force_int_columns and column in force_int_columns:
                        v = -999
                    if force_float_columns and column in force_float_columns:
                        v = -999.0

            results[column] = v
        if unique:
            results['uniq'] = 'uniq-{0}'.format(str(uuid.uuid4())[:8])

        return results

    def __check_match_and_filter(self,
                                 row,
                                 check_columns,
                                 check_by_string,
                                 check_by_regex,
                                 check_type,
                                 csv_file_length=0):
        """Private Function: check_match_and_filter"""

        check_status = dict()
        for k, v in row.items():
            key_status = k in check_columns

            # Init key status
            if key_status and k not in check_status.keys():
                check_status[k] = False

            # Init match count
            if key_status and k not in self.match_count.keys() and check_type == 'match':
                self.match_count[k] = 0

            # Init filter count
            if key_status and k not in self.filter_count.keys() and check_type == 'filter':
                self.filter_count[k] = csv_file_length

            # Check string, regex
            check_by_string_status = v in check_by_string
            check_by_regex_status = any(re.search(the_match, str(v), re.IGNORECASE) for the_match in check_by_regex)
            if key_status and (check_by_string_status or check_by_regex_status):
                check_status[k] = True
                if check_type == 'match':
                    self.match_count[k] += 1
                if check_type == 'filter':
                    self.filter_count[k] -= 1

        # Return status
        value_status = check_status.values()
        if value_status:
            # Default match: check all match
            status = all(value_status)
            if check_type == 'filter':
                # If filter type: check any match
                status = any(value_status)
        else:
            status = False

        # print('')
        # print('-' * 20)
        # print(check_type)
        # print(row)
        # print(check_status)
        # print(value_status)
        # print(status)

        return status

    @staticmethod
    def __validate_columns(csv_headers, check_columns):
        """Private Function: validate_columns """

        if check_columns:
            if len(check_columns) == 1 and check_columns[0] == '*':
                return csv_headers
            validate_check_columns = all(check_column in csv_headers for check_column in check_columns)
            if validate_check_columns is False:
                print('Warning: Not all columns {0} in csv headers {1}. '
                      'Those headers will be ignored.'.format(check_columns, csv_headers))
                check_columns = list(set(csv_headers).intersection(set(check_columns)))

        return check_columns

    @staticmethod
    def __validate_bool_string(target, alias=''):
        """Private Function: Validate bool string

        :param target: the target
        """

        target = str(target).lower()
        expected = ['true', 'false']
        if target not in expected:
            error_message = 'Error: The input {0} should be True or False, current is {1}'.format(alias, target)
            sys.exit(error_message)

        return True if target == 'true' else False

    @staticmethod
    def __unix_time_millis(dt):
        """Private Function: unix_time_millis"""

        epoch_naive = datetime.datetime.utcfromtimestamp(0)
        epoch = timezone('UTC').localize(epoch_naive)
        return int((dt - epoch).total_seconds() * 1000)

    def export_csv_to_influx(self,
                             csv_file,
                             db_name,
                             db_measurement,
                             field_columns,
                             tag_columns=None,
                             db_server_name='localhost:8086',
                             db_user='admin',
                             db_password='admin',
                             time_column='timestamp',
                             time_format='%Y-%m-%d %H:%M:%S',
                             delimiter=',',
                             lineterminator='\n',
                             time_zone='UTC',
                             batch_size=500,
                             limit_string_length_columns=None,
                             limit_length=20,
                             drop_database=False,
                             drop_measurement=False,
                             match_columns=None,
                             match_by_string=None,
                             match_by_regex=None,
                             filter_columns=None,
                             filter_by_string=None,
                             filter_by_regex=None,
                             enable_count_measurement=False,
                             force_insert_even_csv_no_update=True,
                             force_string_columns=None,
                             force_int_columns=None,
                             force_float_columns=None,
                             http_schema='http',
                             org_name='my-org',
                             bucket_name='my-bucket',
                             token=None,
                             unique=False):
        """Function: export_csv_to_influx

        :param csv_file: the csv file path/folder
        :param db_server_name: the influx server (default localhost:8086)
        :param db_user: for 0.x, 1.x only, the influx db user (default admin)
        :param db_password: for 0.x, 1.x only, the influx db password (default admin)
        :param db_name: for 0.x, 1.x only, the influx db name
        :param db_measurement: the measurement in a db
        :param time_column: the time columns (default timestamp)
        :param tag_columns: the tag columns (default None)
        :param time_format: the time format (default %Y-%m-%d %H:%M:%S)
        :param field_columns: the filed columns
        :param delimiter: the csv delimiter (default comma)
        :param lineterminator: the csv line terminator (default comma)
        :param batch_size: how many rows insert every time (default 500)
        :param time_zone: the data time zone (default UTC)
        :param limit_string_length_columns: limit the string length columns (default None)
        :param limit_length: limited length (default 20)
        :param drop_database: drop database (default False)
        :param drop_measurement: drop measurement (default False)
        :param match_columns: the columns need to be matched (default None)
        :param match_by_string: match columns by string (default None)
        :param match_by_regex: match columns by regex (default None)
        :param filter_columns: the columns need to be filter (default None)
        :param filter_by_string: filter columns by string (default None)
        :param filter_by_regex: filter columns by regex (default None)
        :param enable_count_measurement: create the measurement with only count info (default False)
        :param force_insert_even_csv_no_update: force insert data to influx even csv data no update (default True)
        :param force_string_columns: force the columns as string (default None)
        :param force_int_columns: force the columns as int (default None)
        :param force_float_columns: force the columns as float (default None)
        :param http_schema: for 2.x only, influxdb http schema, could be http or https (default http)
        :param org_name: for 2.x only, my org (default my-org)
        :param bucket_name: for 2.x only, my bucket (default my-bucket)
        :param token: for 2.x only, token (default None)
        :param unique: insert the duplicated data (default False)
        """

        # Init: object
        csv_object = CSVObject(delimiter=delimiter, lineterminator=lineterminator)
        influx_object = InfluxObject(db_server_name=db_server_name,
                                     db_user=db_user,
                                     db_password=db_password,
                                     http_schema=http_schema,
                                     token=token)
        client = influx_object.connect_influx_db(db_name=db_name, org_name=org_name)
        influx_version = influx_object.influxdb_version
        base_object = BaseObject()

        # Init: Arguments
        base_object.validate_str(csv_file, target_name='csv_file')
        base_object.validate_str(db_name, target_name='db_name')
        base_object.validate_str(db_measurement, target_name='db_measurement')
        base_object.validate_str(time_format, target_name='time_format')
        base_object.validate_str(delimiter, target_name='delimiter')
        base_object.validate_str(lineterminator, target_name='lineterminater')
        base_object.validate_str(time_zone, target_name='time_zone')
        tag_columns = base_object.str_to_list(tag_columns)
        field_columns = base_object.str_to_list(field_columns)
        limit_string_length_columns = [] if str(limit_string_length_columns).lower() == 'none' \
            else limit_string_length_columns
        limit_string_length_columns = base_object.str_to_list(limit_string_length_columns)
        match_columns = [] if str(match_columns).lower() == 'none' else match_columns
        match_columns = base_object.str_to_list(match_columns)
        match_by_string = [] if str(match_by_string).lower() == 'none' else match_by_string
        match_by_string = base_object.str_to_list(match_by_string)
        match_by_regex = [] if str(match_by_regex).lower() == 'none' else match_by_regex
        match_by_regex = base_object.str_to_list(match_by_regex, lower=False)
        filter_columns = [] if str(filter_columns).lower() == 'none' else filter_columns
        filter_columns = base_object.str_to_list(filter_columns)
        filter_by_string = [] if str(filter_by_string).lower() == 'none' else filter_by_string
        filter_by_string = base_object.str_to_list(filter_by_string)
        filter_by_regex = [] if str(filter_by_regex).lower() == 'none' else filter_by_regex
        filter_by_regex = base_object.str_to_list(filter_by_regex)
        drop_database = self.__validate_bool_string(drop_database)
        drop_measurement = self.__validate_bool_string(drop_measurement)
        enable_count_measurement = self.__validate_bool_string(enable_count_measurement)
        force_insert_even_csv_no_update = self.__validate_bool_string(force_insert_even_csv_no_update)
        force_string_columns = [] if str(force_string_columns).lower() == 'none' else force_string_columns
        force_string_columns = base_object.str_to_list(force_string_columns)
        force_int_columns = [] if str(force_int_columns).lower() == 'none' else force_int_columns
        force_int_columns = base_object.str_to_list(force_int_columns)
        force_float_columns = [] if str(force_float_columns).lower() == 'none' else force_float_columns
        force_float_columns = base_object.str_to_list(force_float_columns)

        # Fields should not duplicate in force_string_columns, force_int_columns, force_float_columns
        all_force_columns = force_string_columns + force_int_columns + force_float_columns
        duplicates = [item for item, count in collections.Counter(all_force_columns).items() if count > 1]
        if duplicates:
            error_message = 'Error: Find duplicate items: {0} in: \n' \
                            '       force_string_columns: {1} \n' \
                            '       force_int_columns: {2} \n' \
                            '       force_float_columns: {3}'.format(duplicates,
                                                                     force_string_columns,
                                                                     force_int_columns,
                                                                     force_float_columns)
            sys.exit(error_message)

        # Init: database behavior
        drop_database = base_object.convert_boole(drop_database)
        drop_measurement = base_object.convert_boole(drop_measurement)
        enable_count_measurement = base_object.convert_boole(enable_count_measurement)
        force_insert_even_csv_no_update = base_object.convert_boole(force_insert_even_csv_no_update)
        count_measurement = '{0}.count'.format(db_measurement)
        if drop_measurement:
            influx_object.drop_measurement(db_name, db_measurement, bucket_name, org_name, client)
            influx_object.drop_measurement(db_name, count_measurement, bucket_name, org_name, client)
        if drop_database:
            if influx_version.startswith('0') or influx_version.startswith('1'):
                influx_object.drop_database(db_name, client)
                influx_object.create_influx_db_if_not_exists(db_name, client)
            else:
                influx_object.drop_bucket(org_name=org_name, bucket_name=bucket_name)
                influx_object.create_influx_bucket_if_not_exists(org_name=org_name, bucket_name=bucket_name)
        if influx_version.startswith('0') or influx_version.startswith('1'):
            client.switch_user(db_user, db_password)

        # Init: batch_size
        try:
            batch_size = int(batch_size)
        except ValueError:
            error_message = 'Error: The batch_size should be int, current is: {0}'.format(batch_size)
            sys.exit(error_message)

        # Init: limit_length
        try:
            limit_length = int(limit_length)
        except ValueError:
            error_message = 'Error: The limit_length should be int, current is: {0}'.format(limit_length)
            sys.exit(error_message)

        # Process csv_file
        current_dir = os.path.curdir
        csv_file = os.path.join(current_dir, csv_file)
        csv_file_exists = os.path.exists(csv_file)
        if csv_file_exists is False:
            error_message = 'Error: CSV file not found, exiting...'
            sys.exit(error_message)
        csv_file_generator = csv_object.search_files_in_dir(csv_file)
        for csv_file_item in csv_file_generator:
            csv_file_length = csv_object.get_csv_lines_count(csv_file_item)
            csv_file_md5 = csv_object.get_file_md5(csv_file_item)
            csv_headers = csv_object.get_csv_header(csv_file_item)

            # Validate csv_headers
            if not csv_headers:
                print('Error: The csv file {0} has no header detected. Exporter stopping...'.format(csv_file_item))
                continue

            # Validate field_columns, tag_columns, match_columns, filter_columns
            field_columns = self.__validate_columns(csv_headers, field_columns)
            tag_columns = self.__validate_columns(csv_headers, tag_columns)
            if not field_columns:
                print('Error: The input --field_columns does not expected. '
                      'Please check the fields are in csv headers or not. Exporter stopping...')
                continue
            if not tag_columns:
                print('Warning: The input --tag_columns does not expected or leaves None. '
                      'Please check the fields are in csv headers or not. Continue checking...')
                # continue
            match_columns = self.__validate_columns(csv_headers, match_columns)
            filter_columns = self.__validate_columns(csv_headers, filter_columns)

            # Validate time_column
            with open(csv_file_item) as f:
                csv_reader = csv.DictReader(f, delimiter=delimiter, lineterminator=lineterminator)
                time_column_exists = True
                for row in csv_reader:
                    try:
                        time_column_exists = row[time_column]
                    except KeyError:
                        print('Warning: The time column does not exists. '
                              'We will use the csv last modified time as time column')
                        time_column_exists = False
                    break

            # Check the timestamp, and generate the csv with checksum
            new_csv_file = '{0}_influx.csv'.format(csv_file_item.replace('.csv', ''))
            new_csv_file_exists = os.path.exists(new_csv_file)
            no_new_data_status = False
            if new_csv_file_exists:
                with open(new_csv_file) as f:
                    csv_reader = csv.DictReader(f, delimiter=delimiter, lineterminator=lineterminator)
                    for row in csv_reader:
                        try:
                            new_csv_file_md5 = row['md5']
                        except KeyError:
                            break
                        if new_csv_file_md5 == csv_file_md5 and force_insert_even_csv_no_update is False:
                            warning_message = 'Warning: No new data found, ' \
                                              'exporter stop/jump for {0}...'.format(csv_file_item)
                            print(warning_message)
                            no_new_data_status = True
                            # sys.exit(warning_message)
                        break
            if no_new_data_status:
                continue
            data = [{'md5': [csv_file_md5] * csv_file_length}]
            if time_column_exists is False:
                modified_time = csv_object.get_file_modify_time(csv_file_item)
                field_columns.append('timestamp')
                tag_columns.append('timestamp')
                data.append({time_column: [modified_time] * csv_file_length})
            csv_reader_data = csv_object.add_columns_to_csv(file_name=csv_file_item,
                                                            target=new_csv_file,
                                                            data=data,
                                                            save_csv_file=not force_insert_even_csv_no_update)

            # Process influx csv
            data_points = list()
            count = 0
            timestamp = 0
            convert_csv_data_to_int_float = csv_object.convert_csv_data_to_int_float(csv_reader=csv_reader_data)
            for row, int_type, float_type in convert_csv_data_to_int_float:
                # Process Match & Filter: If match_columns exists and filter_columns not exists
                match_status = self.__check_match_and_filter(row,
                                                             match_columns,
                                                             match_by_string,
                                                             match_by_regex,
                                                             check_type='match')
                filter_status = self.__check_match_and_filter(row,
                                                              filter_columns,
                                                              filter_by_string,
                                                              filter_by_regex,
                                                              check_type='filter',
                                                              csv_file_length=csv_file_length)
                if match_columns and not filter_columns:
                    if match_status is False:
                        continue

                # Process Match & Filter: If match_columns not exists and filter_columns exists
                if not match_columns and filter_columns:
                    if filter_status is True:
                        continue

                # Process Match & Filter: If match_columns, filter_columns both exists
                if match_columns and filter_columns:
                    if match_status is False and filter_status is True:
                        continue

                # Process Time
                try:
                    # raise if not posix-time-like
                    timestamp_str = str(float(row[time_column]))
                    timestamp_magnitude = len(timestamp_str.split('.')[0])
                    timestamp_remove_decimal = int(
                        str(timestamp_str).replace('.', '')
                    )
                    # add zeros to convert to nanoseconds
                    timestamp_influx = (
                        '{:<0'+str(9+timestamp_magnitude)+'d}'
                    ).format(timestamp_remove_decimal)
                    timestamp = int(timestamp_influx)
                except ValueError:
                    try:
                        datetime_naive = datetime.datetime.strptime(row[time_column], time_format)
                        datetime_local = timezone(time_zone).localize(datetime_naive)
                        timestamp = self.__unix_time_millis(datetime_local) * 1000000
                    except (TypeError, ValueError):
                        error_message = 'Error: Unexpected time with format: {0}, {1}'.format(row[time_column],
                                                                                              time_format)
                        sys.exit(error_message)

                # Process tags
                tags = self.__process_tags_fields(columns=tag_columns,
                                                  row=row,
                                                  limit_length=limit_length,
                                                  int_type=int_type,
                                                  float_type=float_type,
                                                  limit_string_length_columns=limit_string_length_columns,
                                                  force_string_columns=force_string_columns,
                                                  force_int_columns=force_int_columns,
                                                  force_float_columns=force_float_columns,
                                                  unique=unique)

                # Process fields
                fields = self.__process_tags_fields(columns=field_columns,
                                                    row=row,
                                                    limit_length=limit_length,
                                                    int_type=int_type,
                                                    float_type=float_type,
                                                    limit_string_length_columns=limit_string_length_columns,
                                                    force_string_columns=force_string_columns,
                                                    force_int_columns=force_int_columns,
                                                    force_float_columns=force_float_columns,
                                                    unique=False)

                point = {'measurement': db_measurement, 'time': timestamp, 'fields': fields, 'tags': tags}
                data_points.append(point)

                count += 1

                # Write points
                data_points_len = len(data_points)
                if data_points_len % batch_size == 0:
                    print('Info: Read {0} lines from {1}'.format(count, csv_file_item))
                    print('Info: Inserting {0} data_points...'.format(data_points_len))
                    try:
                        if influx_version.startswith('0') or influx_version.startswith('1'):
                            self._write_response = client.write_points(data_points)
                        else:
                            with client.write_api(write_options=WriteOptions(batch_size=batch_size),
                                                  error_callback=self.__error_cb) as write_client:
                                write_client.write(bucket_name, org_name, data_points)
                    except influx_object.influxdb_client_error as e:
                        error_message = 'Error: System exited. Encounter data type conflict issue in influx. \n' \
                                        '       Please double check the csv data. \n' \
                                        '       If would like to force data type to target data type, use: \n' \
                                        '       --force_string_columns \n' \
                                        '       --force_int_columns \n' \
                                        '       --force_float_columns \n' \
                                        '       Error Details: {0}'.format(e)
                        sys.exit(error_message)
                    if self._write_response is False:
                        error_message = 'Info: Problem inserting points, exiting...'
                        sys.exit(error_message)
                    print('Info: Wrote {0} points'.format(data_points_len))
                    data_points = list()

            # Write rest points
            data_points_len = len(data_points)
            if data_points_len > 0:
                print('Info: Read {0} lines from {1}'.format(count, csv_file_item))
                print('Info: Inserting {0} data_points...'.format(data_points_len))
                try:
                    if influx_version.startswith('0') or influx_version.startswith('1'):
                        self._write_response = client.write_points(data_points)
                    else:
                        with client.write_api(write_options=WriteOptions(batch_size=batch_size),
                                              error_callback=self.__error_cb) as write_client:
                            write_client.write(bucket_name, org_name, data_points)
                except influx_object.influxdb_client_error as e:
                    error_message = 'Error: System exited. Encounter data type conflict issue in influx. \n' \
                                    '       Please double check the csv data. \n' \
                                    '       If would like to force data type to target data type, use: \n' \
                                    '       --force_string_columns \n' \
                                    '       --force_int_columns \n' \
                                    '       --force_float_columns \n' \
                                    '       Error Details: {0}'.format(e)
                    sys.exit(error_message)
                if self._write_response is False:
                    error_message = 'Error: Problem inserting points, exiting...'
                    sys.exit(error_message)
                print('Info: Wrote {0} points'.format(data_points_len))

            # Write count measurement
            if enable_count_measurement:
                fields = dict()
                fields['total'] = csv_file_length
                for k, v in self.match_count.items():
                    k = 'match_{0}'.format(k)
                    fields[k] = v
                for k, v in self.filter_count.items():
                    k = 'filter_{0}'.format(k)
                    fields[k] = v
                count_point = [{'measurement': count_measurement, 'time': timestamp, 'fields': fields, 'tags': {}}]
                if influx_version.startswith('0') or influx_version.startswith('1'):
                    self._write_response = client.write_points(count_point)
                else:
                    with client.write_api(write_options=WriteOptions(batch_size=batch_size),
                                          error_callback=self.__error_cb) as write_client:
                        write_client.write(bucket_name, org_name, count_point)
                print('Info: Wrote count measurement {0} points'.format(count_point))

                self.match_count = defaultdict(int)
                self.filter_count = defaultdict(int)
            if self._write_response is False:
                error_message = 'Error: Problem inserting points, exiting...'
                sys.exit(error_message)
            print('Info: Done')
            print('')


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
    exporter.export_csv_to_influx(csv_file=args.csv,
                                  db_server_name=user_namespace.server,
                                  db_user=args.user,
                                  db_password=args.password,
                                  db_name='None' if args.dbname is None else args.dbname,
                                  db_measurement=args.measurement,
                                  time_column=args.time_column,
                                  time_format=args.time_format,
                                  time_zone=args.time_zone,
                                  field_columns=args.field_columns,
                                  tag_columns=args.tag_columns,
                                  batch_size=args.batch_size,
                                  delimiter=args.delimiter,
                                  lineterminator=args.lineterminator,
                                  limit_string_length_columns=args.limit_string_length_columns,
                                  limit_length=args.limit_length,
                                  drop_database=args.drop_database,
                                  drop_measurement=args.drop_measurement,
                                  match_columns=args.match_columns,
                                  match_by_string=args.match_by_string,
                                  match_by_regex=args.match_by_regex,
                                  filter_columns=args.filter_columns,
                                  filter_by_string=args.filter_by_string,
                                  filter_by_regex=args.filter_by_regex,
                                  enable_count_measurement=args.enable_count_measurement,
                                  force_insert_even_csv_no_update=args.force_insert_even_csv_no_update,
                                  force_string_columns=args.force_string_columns,
                                  force_int_columns=args.force_int_columns,
                                  force_float_columns=args.force_float_columns,
                                  http_schema=args.http_schema,
                                  org_name=args.org,
                                  bucket_name=args.bucket,
                                  token='None' if args.token is None else args.token,
                                  unique=args.unique)
