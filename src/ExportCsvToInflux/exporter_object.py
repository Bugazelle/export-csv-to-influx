from .influx_object import InfluxObject
from collections import defaultdict
from .base_object import BaseObject
from .__version__ import __version__
from .csv_object import CSVObject
from pytz import timezone
import argparse
import datetime
import csv
import sys
import os
import re


class ExporterObject(object):
    """ExporterObject"""

    def __init__(self):
        self.match_count = defaultdict(int)
        self.filter_count = defaultdict(int)

    @staticmethod
    def convert_boole(target):
        target = str(target).lower()
        if target != 'true' and target != 'false':
            raise Exception('Error: The expected input for {0} should be: True or False'.format(target))
        if target == 'true':
            target = True
        else:
            target = False

        return target

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
            validate_check_columns = all(check_column in csv_headers for check_column in check_columns)
            if validate_check_columns is False:
                print('Warning: Not all columns {0} in csv headers {1}. '
                      'Those headers will be ignored.'.format(check_columns, csv_headers))
                check_columns = list(set(csv_headers).intersection(set(check_columns)))

        return check_columns

    @staticmethod
    def __unix_time_millis(dt):
        """Private Function: unix_time_millis"""

        epoch_naive = datetime.datetime.utcfromtimestamp(0)
        epoch = timezone('UTC').localize(epoch_naive)
        return int((dt - epoch).total_seconds() * 1000)

    def export_csv_to_influx(self,
                             csv_file,
                             db_server_name,
                             db_user,
                             db_password,
                             db_name,
                             db_measurement,
                             tag_columns,
                             field_columns,
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
                             force_insert_even_csv_no_update=False):
        """Function: export_csv_to_influx

        :param csv_file: the csv file path/folder
        :param db_server_name: the influx server
        :param db_user: the influx db user
        :param db_password: the influx db password
        :param db_name: the influx db name
        :param db_measurement: the measurement in a db
        :param time_column: the time columns (default timestamp)
        :param tag_columns: the tag columns ()
        :param time_format: the time format (default %Y-%m-%d %H:%M:%S)
        :param field_columns: the filed columns
        :param delimiter: the csv delimiter (default comma)
        :param lineterminator: the csv line terminator (default comma)
        :param batch_size: how many rows insert every time (default 500)
        :param time_zone: the data time zone (default UTC)
        :param limit_string_length_columns: limit the string length
        :param limit_length: default 20
        :param drop_database: drop database (default False)
        :param drop_measurement: drop measurement (default False)
        :param match_columns: the columns need to be matched (default None)
        :param match_by_string: match columns by string (default None)
        :param match_by_regex: match columns by regex (default None)
        :param filter_columns: the columns need to be filter (default None)
        :param filter_by_string: filter columns by string (default None)
        :param filter_by_regex: filter columns by regex (default None)
        :param enable_count_measurement: create the measurement with only count info
        :param force_insert_even_csv_no_update: force insert data to influx even csv data no update (default False)
        """

        # Init: object
        csv_object = CSVObject(delimiter=delimiter, lineterminator=lineterminator)
        influx_object = InfluxObject(db_server_name=db_server_name, db_user=db_user, db_password=db_password)
        base_object = BaseObject()

        # Init: Arguments
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

        # Init: database behavior
        drop_database = self.convert_boole(drop_database)
        drop_measurement = self.convert_boole(drop_measurement)
        enable_count_measurement = self.convert_boole(enable_count_measurement)
        force_insert_even_csv_no_update = self.convert_boole(force_insert_even_csv_no_update)
        count_measurement = '{0}.count'.format(db_measurement)
        if drop_measurement:
            influx_object.drop_measurement(db_name, db_measurement)
            influx_object.drop_measurement(db_name, count_measurement)
        if drop_database:
            influx_object.drop_database(db_name)
        client = influx_object.create_influx_db_if_not_exists(db_name)
        client.switch_user(db_user, db_password)

        # Init: batch_size
        try:
            batch_size = int(batch_size)
        except ValueError:
            raise Exception('Error: The batch_size should be int, current is: {0}'.format(batch_size))

        # Process csv_file
        current_dir = os.path.curdir
        csv_file = os.path.join(current_dir, csv_file)
        csv_file_exists = os.path.exists(csv_file)
        if csv_file_exists is False:
            print('Info: CSV file not found, exiting...')
            sys.exit(0)
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
            if not tag_columns:
                print('Error: The input --tag_columns does not expected. '
                      'Please check the fields are in csv headers or not. Exporter stopping...')
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
            new_csv_file = 'influx.csv'
            new_csv_file = os.path.join(current_dir, new_csv_file)
            new_csv_file_exists = os.path.exists(new_csv_file)
            no_new_data_status = False
            if new_csv_file_exists:
                with open(new_csv_file) as f:
                    csv_reader = csv.DictReader(f, delimiter=delimiter, lineterminator=lineterminator)
                    for row in csv_reader:
                        new_csv_file_md5 = row['md5']
                        if new_csv_file_md5 == csv_file_md5 and force_insert_even_csv_no_update is False:
                            print('Info: No new data found, existing...')
                            no_new_data_status = True
                            # sys.exit(0)
                        break
            if no_new_data_status:
                continue
            data = [{'md5': [csv_file_md5] * csv_file_length}]
            if time_column_exists is False:
                modified_time = csv_object.get_file_modify_time(csv_file_item)
                data.append({time_column: [modified_time] * csv_file_length})
            csv_object.add_columns_to_csv(file_name=csv_file_item, target=new_csv_file, data=data)

            # Open influx csv
            data_points = list()
            count = 0
            timestamp = 0
            convert_csv_data_to_int_float = csv_object.convert_csv_data_to_int_float(file_name=new_csv_file)
            for row in convert_csv_data_to_int_float:
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
                datetime_naive = datetime.datetime.strptime(row[time_column], time_format)
                datetime_local = timezone(time_zone).localize(datetime_naive)
                timestamp = self.__unix_time_millis(datetime_local) * 1000000

                # Process tags
                tags = dict()
                for tag_column in tag_columns:
                    v = 0
                    if tag_column in row:
                        v = row[tag_column]
                        if limit_string_length_columns:
                            if tag_column in limit_string_length_columns:
                                v = str(v)[:limit_length + 1]
                    tags[tag_column] = v

                # Process fields
                fields = dict()
                for field_column in field_columns:
                    v = 0
                    if field_column in row:
                        v = row[field_column]
                        if limit_string_length_columns:
                            if field_column in limit_string_length_columns:
                                v = str(v)[:limit_length + 1]
                    fields[field_column] = v

                point = {'measurement': db_measurement, 'time': timestamp, 'fields': fields, 'tags': tags}
                data_points.append(point)

                count += 1

                # Write points
                data_points_len = len(data_points)
                if data_points_len % batch_size == 0:
                    print('Info: Read {0} lines from {1}'.format(count, csv_file_item))
                    print('Info: Inserting {0} data_points...'.format(data_points_len))
                    response = client.write_points(data_points)
                    if response is False:
                        print('Info: Problem inserting points, exiting...')
                        exit(1)
                    print('Info: Wrote {0} lines, response: {1}'.format(data_points_len, response))

                    data_points = list()

            # Write rest points
            data_points_len = len(data_points)
            if data_points_len > 0:
                print('Info: Read {0} lines from {1}'.format(count, csv_file_item))
                print('Info: Inserting {0} data_points...'.format(data_points_len))
                response = client.write_points(data_points)
                if response is False:
                    print('Error: Problem inserting points, exiting...')
                    exit(1)
                print('Info: Wrote {0}, response: {1}'.format(data_points_len, response))

            # Write count measurement
            fields = dict()
            fields['total'] = csv_file_length
            for k, v in self.match_count.items():
                k = 'match_{0}'.format(k)
                fields[k] = v
            for k, v in self.filter_count.items():
                k = 'filter_{0}'.format(k)
                fields[k] = v
            count_point = [{'measurement': count_measurement, 'time': timestamp, 'fields': fields, 'tags': None}]
            if enable_count_measurement:
                response = client.write_points(count_point)
                if response is False:
                    print('Error: Problem inserting points, exiting...')
                    exit(1)
                print('Info: Wrote count measurement {0}, response: {1}'.format(count_point, response))

            self.match_count = defaultdict(int)
            self.filter_count = defaultdict(int)

            print('Info: Done')
            print('')


def export_csv_to_influx():
    parser = argparse.ArgumentParser(description='CSV to InfluxDB.')
    parser.add_argument('-c', '--csv', required=True,
                        help='Input CSV file.')
    parser.add_argument('-d', '--delimiter', nargs='?', default=',', const=',',
                        help='CSV delimiter. Default: \',\'.')
    parser.add_argument('-lt', '--lineterminator', nargs='?', default='\n', const='\n',
                        help='CSV lineterminator. Default: \'\\n\'.')
    parser.add_argument('-s', '--server', nargs='?', default='localhost:8086', const='localhost:8086',
                        help='InfluxDB Server address. Default: localhost:8086')
    parser.add_argument('-u', '--user', nargs='?', default='admin', const='admin',
                        help='InfluxDB User name.')
    parser.add_argument('-p', '--password', nargs='?', default='admin', const='admin',
                        help='InfluxDB Password.')
    parser.add_argument('-db', '--dbname', required=True,
                        help='InfluxDB Database name.')
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
    parser.add_argument('-tc', '--tag_columns', required=True,
                        help='List of csv columns to use as tags, separated by comma')
    parser.add_argument('-b', '--batch_size', nargs='?', default=500, const=500,
                        help='Batch size when inserting data to influx. Default: 500.')
    parser.add_argument('-lslc', '--limit_string_length_columns',nargs='?',  default=None, const=None,
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
    parser.add_argument('-fi', '--force_insert_even_csv_no_update', nargs='?', default=False, const=False,
                        help='Force insert data to influx, even csv no update. Default: False')
    parser.add_argument('-v', '--version', action="version", version=__version__)

    args = parser.parse_args()
    exporter = ExporterObject()
    exporter.export_csv_to_influx(csv_file=args.csv,
                                  db_server_name=args.server,
                                  db_user=args.user,
                                  db_password=args.password,
                                  db_name=args.dbname,
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
                                  force_insert_even_csv_no_update=args.force_insert_even_csv_no_update)
