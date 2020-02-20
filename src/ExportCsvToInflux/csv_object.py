from collections import defaultdict
from .base_object import BaseObject
from itertools import tee
from glob import glob
import hashlib
import types
import time
import json
import csv
import sys
import os


class CSVObject(object):
    """CSV Object"""

    def __init__(self, delimiter=',', lineterminator='\n'):
        self.delimiter = delimiter
        self.lineterminator = lineterminator

    def get_csv_header(self, file_name):
        """Function: get_csv_header.

        :param file_name: the file name
        :return return csv header as list

        """

        self.valid_file_exist(file_name)

        with open(file_name) as f:
            sniffer = csv.Sniffer()
            try:
                has_header = sniffer.has_header(f.read(40960))
            except csv.Error:
                has_header = False
            f.seek(0)
            csv_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            headers = csv_reader.fieldnames
            is_header = not any(field.isdigit() for field in headers)
            headers = headers if has_header or is_header else []

            return headers

    @staticmethod
    def search_files_in_dir(directory, match_suffix='.csv', filter_pattern='_influx.csv'):
        """Function: search_files_in_dir

        :param directory: the directory
        :param match_suffix: match the file suffix, use comma to separate, only string, not support regex
        :param filter_pattern: filter the files, only string, not support regex
        """

        base_object = BaseObject()
        match_suffix = base_object.str_to_list(match_suffix, lower=True)
        filter_pattern = base_object.str_to_list(filter_pattern, lower=True)

        # Is file
        is_file = os.path.isfile(directory)
        if is_file:
            yield directory

        # Search directory
        for x in os.walk(directory):
            for y in glob(os.path.join(x[0], '*.*')):
                # Continue if directory
                try:
                    check_directory = os.path.isdir(y)
                except UnicodeEncodeError as e:
                    y = y.encode('utf-8', 'ignore')
                    print('Warning: Unicode Encode Error found when checking isdir {0}: {1}'.format(y, e))
                    check_directory = os.path.isdir(y)
                if check_directory is True:
                    continue
                # Filter Out
                match_suffix_status = any(the_filter in y.lower() for the_filter in match_suffix)
                filter_pattern_status = any(the_filter in y.lower() for the_filter in filter_pattern)
                if match_suffix_status is True and filter_pattern_status is False:
                    yield y

    @staticmethod
    def valid_file_exist(file_name):
        """Function: valid_file_exist

        :param file_name: the file name
        """

        file_exists = os.path.exists(file_name)
        if file_exists is False:
            error_message = 'Error: The file does not exist: {0}'.format(file_name)
            sys.exit(error_message)

    def get_file_md5(self, file_name):
        """Function: get_file_md5

        :param file_name: the file name
        :return return the file md5
        """

        self.valid_file_exist(file_name)

        hash_md5 = hashlib.md5()
        with open(file_name, "rb") as f:
            for chunk in iter(lambda: f.read(40960), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    def get_file_modify_time(self, file_name, enable_ms=False):
        """Function: get_file_modify_time

        :param file_name: the file name
        :param enable_ms: enable milliseconds (default False)
        :return return the human readable time
        """

        self.valid_file_exist(file_name)

        modified = os.path.getmtime(file_name)
        modified_s, modified_ms = divmod(modified * 1000, 1000)
        if enable_ms is False:
            modified_pretty = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(modified_s))
        else:
            modified_pretty = '%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(modified_s)), modified_ms)

        return modified_pretty

    def get_csv_lines_count(self, file_name):
        """Function: get_csv_lines_count.

        :param file_name: the file name
        :return return csv line count. No count header into count

        """

        has_header = self.get_csv_header(file_name)

        with open(file_name) as f:
            csv_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            count = 0 if has_header else 1
            for row in csv_reader:
                count += 1

            return count

    def convert_csv_data_to_int_float(self, file_name=None, csv_reader=None):
        """Function: convert_csv_data_to_int_float

        :param file_name: the file name (default None)
        :param csv_reader: the csv dict reader (default None)
            The csv_reader could come from 2 ways:
            1. use csv.DictReader to get the csv_reader object
            2. use dict to make up the csv_reader, the dict format is as following
                [
                    {'csv_header_1': 'value', 'csv_header_2': 'value', 'csv_header_3': 'value', ...},
                    {'csv_header_1': 'value', 'csv_header_2': 'value', 'csv_header_3': 'value', ...},
                    {'csv_header_1': 'value', 'csv_header_2': 'value', 'csv_header_3': 'value', ...},
                    ...
                ]
        """

        # init
        int_type = defaultdict(list)
        float_type = defaultdict(list)
        keys = list()
        csv_reader = list() if csv_reader is None else csv_reader
        csv_reader_bk = csv_reader
        has_header = True

        # Verify the csv_reader
        csv_reader_type = type(csv_reader)
        is_generator_type = isinstance(csv_reader, types.GeneratorType)
        if csv_reader_type != list and csv_reader_type != csv.DictReader and not is_generator_type:
            error_message = 'Error: The csv_reader type is not expected: {0}, ' \
                            'should list type or csv.DictReader'.format(csv_reader_type)
            sys.exit(error_message)
        if is_generator_type:
            csv_reader, csv_reader_bk = tee(csv_reader)

        # Get csv_reader from csv file
        f = None
        if file_name:
            has_header = self.get_csv_header(file_name)
            f = open(file_name)
            csv_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            csv_reader, csv_reader_bk = tee(csv_reader)

        # Process
        for row in csv_reader:
            keys = row.keys()
            for key in keys:
                value = row[key]
                len_value = len(value)
                # Continue If Value Empty
                if len_value == 0:
                    continue
                # Valid Int Type
                try:
                    if float(value).is_integer():
                        int_type[key].append(True)
                    else:
                        int_type[key].append(False)
                except ValueError:
                    int_type[key].append(False)
                # Valid Float Type
                try:
                    float(value)
                    float_type[key].append(True)
                except ValueError:
                    float_type[key].append(False)

        # Valid the key if no header
        if keys and not has_header:
            for key in keys:
                len_key = len(key)
                # Continue If Key Empty
                if len_key == 0:
                    continue
                # Valid Int Type
                try:
                    if float(key).is_integer():
                        int_type[key].append(True)
                    else:
                        int_type[key].append(False)
                except ValueError:
                    int_type[key].append(False)
                # Valid Float Type
                try:
                    float(key)
                    float_type[key].append(True)
                except ValueError:
                    float_type[key].append(False)

        # Finalize Type
        int_type = {k: all(int_type[k]) for k in int_type}
        float_type = {k: all(float_type[k]) for k in float_type}

        # Yield Data
        i = 1
        for row in csv_reader_bk:
            keys = row.keys()
            for key in keys:
                value = row[key]
                int_status = int_type[key]
                len_value = len(value)
                if len_value == 0:
                    continue
                if int_status is True:
                    row[key] = int(float(value)) if int_type[key] is True else value
                else:
                    row[key] = float(value) if float_type[key] is True else value
            yield row, int_type, float_type
            if not has_header and i == 1:
                for key in keys:
                    int_status = int_type[key]
                    len_key = len(key)
                    if len_key == 0:
                        continue
                    if int_status is True:
                        row[key] = int(float(key)) if int_type[key] is True else key
                    else:
                        row[key] = float(key) if float_type[key] is True else key
                yield row, int_type, float_type
            i += 1

        # Close file
        if file_name:
            f.close()

    def add_columns_to_csv(self,
                           file_name,
                           target,
                           data,
                           save_csv_file=True):
        """Function: add_columns_to_csv

        :param file_name: the file name
        :param target: the target file to save result
        :param data: the new columns data, list type, the item is dict.
            for example: [{"new_header_1": ["new_value_1", "new_value_2", "new_value_3"]},
                          {"new_header_2": ["new_value_1", "new_value_2", "new_value_3"]}
                         ]
        :param save_csv_file: save csv file to local (default True)
        :return return the new csv data by dict
        """

        has_header = self.get_csv_header(file_name)

        # Process data
        data_type = type(data)
        error_message = 'Error: The data should be list type, the item should be dict. Or the json type as following ' \
                        'for example: [{"new_header_1": ["new_value_1", "new_value_2", "new_value_3"]}, ' \
                        '{"new_header_2": ["new_value_1", "new_value_2", "new_value_3"]}]'
        try:
            check_data_type = data_type is not list and data_type is not str and data_type is not unicode
        except NameError:
            check_data_type = data_type is not list and data_type is not str
        if check_data_type:
            sys.exit(error_message)

        try:
            check_data_type = data_type is str or data_type is unicode
        except NameError:
            check_data_type = data_type is str
        if check_data_type:
            try:
                data = json.loads(data)
            except ValueError:
                sys.exit(error_message)

        # Add columns
        target_writer = None
        target_file = None
        if save_csv_file:
            target_file = open(target, 'w+')
            target_writer = csv.writer(target_file, delimiter=self.delimiter, lineterminator=self.lineterminator)

        with open(file_name) as f:
            source_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            new_headers = [list(x.keys())[0] for x in data]
            row_id = 0
            for row in source_reader:
                values = list(row.values())
                if row_id == 0:
                    headers = list(row.keys())
                    if not has_header:
                        continue
                    headers += new_headers
                    if save_csv_file:
                        target_writer.writerow(headers)
                new_values = list()
                for x in data:
                    try:
                        value = list(x.values())[0][row_id]
                    except IndexError:
                        print('Warning: The provided column length is less than with the source csv length. '
                              'Use "null" to fill the empty data')
                        value = 'null'
                    new_values.append(value)
                values += new_values
                row_id += 1
                if save_csv_file:
                    target_writer.writerow(values)

                yield dict(zip(headers, values))

        if save_csv_file:
            target_file.close()
