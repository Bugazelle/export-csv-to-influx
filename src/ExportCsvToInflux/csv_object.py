from collections import defaultdict
from .base_object import BaseObject
from glob import glob
import hashlib
import time
import json
import csv
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

        self.valid_file_exit(file_name)

        with open(file_name) as f:
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(f.read(40960))
            f.seek(0)
            csv_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            headers = csv_reader.fieldnames if has_header else []

            return headers

    @staticmethod
    def search_files_in_dir(directory, match_suffix='.csv', filter_pattern='influx.csv'):
        """Function: search_files_in_dir

        :param directory: the directory
        :param match_suffix: match the file suffix, use comma to separate, only string, not support regex
        :param filter_pattern: filter the files, only string, not support regex
        """

        base_object = BaseObject()
        match_suffix = base_object.str_to_list(match_suffix)
        filter_pattern = base_object.str_to_list(filter_pattern)

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
    def valid_file_exit(file_name):
        """Function: valid_file_exit

        :param file_name: the file name
        """

        file_exists = os.path.exists(file_name)
        if file_exists is False:
            raise Exception('Error: The file does not exist: {0}'.format(file_name))

    def get_file_md5(self, file_name):
        """Function: get_file_md5

        :param file_name: the file name
        :return return the file md5
        """

        self.valid_file_exit(file_name)

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

        self.valid_file_exit(file_name)

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

        self.valid_file_exit(file_name)

        with open(file_name) as f:
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(f.read(40960))
            f.seek(0)
            csv_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            count = 0 if has_header is True else 1
            for row in csv_reader:
                count += 1

            return count

    def convert_csv_data_to_int_float(self, file_name):
        """Function: convert_csv_data_to_int_float

        :param file_name: the file name
        """

        self.valid_file_exit(file_name)

        with open(file_name) as f:
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(f.read(40960))
            f.seek(0)
            csv_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            int_type = defaultdict(list)
            float_type = defaultdict(list)
            keys = list()
            for row in csv_reader:
                keys = row.keys()
                for key in keys:
                    # Valid Int Type
                    try:
                        if float(row[key]).is_integer():
                            int_type[key].append(True)
                        else:
                            int_type[key].append(False)
                    except ValueError:
                        int_type[key].append(False)
                    # Valid Float Type
                    try:
                        float(row[key])
                        float_type[key].append(True)
                    except ValueError:
                        float_type[key].append(False)

            # Valid the key if no header
            if keys and has_header is False:
                for key in keys:
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
            f.seek(0)
            csv_reader = csv.DictReader(f)
            i = 1
            for row in csv_reader:
                keys = row.keys()
                for key in keys:
                    value = row[key]
                    int_status = int_type[key]
                    if int_status is True:
                        row[key] = int(float(value)) if int_type[key] is True else value
                    else:
                        row[key] = float(value) if float_type[key] is True else value
                yield row
                if has_header is False and i == 1:
                    for key in keys:
                        int_status = int_type[key]
                        if int_status is True:
                            row[key] = int(float(key)) if int_type[key] is True else key
                        else:
                            row[key] = float(key) if float_type[key] is True else key
                    yield row
                i += 1

    def add_columns_to_csv(self,
                           file_name,
                           target,
                           data):
        """Function: add_columns_to_csv

        :param file_name: the file name
        :param target: the target file to save result
        :param data: the new columns data, list type, the item is dict.
            for example: [{"new_header_1": ["new_value_1", "new_value_2", "new_value_3"]},
                          {"new_header_2": ["new_value_1", "new_value_2", "new_value_3"]}
                         ]
        """

        self.valid_file_exit(file_name)

        # Process data
        data_type = type(data)
        message = 'Error: The data should be list type, the item should be dict. Or the json type as following' \
                  'for example: [{"new_header_1": ["new_value_1", "new_value_2", "new_value_3"]}, ' \
                  '{"new_header_2": ["new_value_1", "new_value_2", "new_value_3"]}]'
        try:
            check_data_type = data_type is not list and data_type is not str and data_type is not unicode
        except NameError:
            check_data_type = data_type is not list and data_type is not str
        if check_data_type:
            raise Exception(message)

        try:
            check_data_type = data_type is str or data_type is unicode
        except NameError:
            check_data_type = data_type is str
        if check_data_type:
            try:
                data = json.loads(data)
            except ValueError:
                raise Exception(message)

        # Add columns
        with open(file_name) as f:
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(f.read(40960))
            f.seek(0)
            source_reader = csv.DictReader(f, delimiter=self.delimiter, lineterminator=self.lineterminator)
            new_headers = [x.keys()[0] for x in data]
            with open(target, 'w+') as target_file:
                target_writer = csv.writer(target_file, delimiter=self.delimiter, lineterminator=self.lineterminator)
                row_id = 0
                for row in source_reader:
                    values = row.values()
                    if row_id == 0:
                        headers = row.keys()
                        if has_header is False:
                            continue
                        headers += new_headers
                        target_writer.writerow(headers)
                    new_values = list()
                    for x in data:
                        try:
                            value = x.values()[0][row_id]
                        except IndexError:
                            print('Warning: The provided column length is less than with the source csv length. '
                                  'Use "null" to fill the empty data')
                            value = 'null'
                        new_values.append(value)
                    values += new_values
                    row_id += 1
                    target_writer.writerow(values)
