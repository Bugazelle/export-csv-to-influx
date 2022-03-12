from .base_object import BaseObject
import collections
import sys
import os


class Configuration(object):
    """Configuration"""

    def __init__(self, **kwargs):
        # Init conf
        self.csv_file = kwargs.get('csv_file', None)
        self.db_name = kwargs.get('db_name', None)
        self.db_measurement = kwargs.get('db_measurement', None)
        self.field_columns = kwargs.get('field_columns', None)
        self.tag_columns = kwargs.get('tag_columns', None)
        self.db_server_name = kwargs.get('db_server_name', 'localhost:8086')
        self.db_user = kwargs.get('db_user', 'admin')
        self.db_password = kwargs.get('db_password', 'admin')
        self.time_column = kwargs.get('time_column', 'timestamp')
        self.time_format = kwargs.get('time_format', '%Y-%m-%d %H:%M:%S')
        self.delimiter = kwargs.get('delimiter', ',')
        self.lineterminator = kwargs.get('lineterminator', '\n')
        self.time_zone = kwargs.get('time_zone', 'UTC')
        self.batch_size = kwargs.get('batch_size', 500)
        self.limit_string_length_columns = kwargs.get('limit_string_length_columns', None)
        self.limit_length = kwargs.get('limit_length', 20)
        self.drop_database = kwargs.get('drop_database', False)
        self.drop_measurement = kwargs.get('drop_measurement', False)
        self.match_columns = kwargs.get('match_columns', None)
        self.match_by_string = kwargs.get('match_by_string', None)
        self.match_by_regex = kwargs.get('match_by_regex', None)
        self.filter_columns = kwargs.get('filter_columns', None)
        self.filter_by_string = kwargs.get('filter_by_string', None)
        self.filter_by_regex = kwargs.get('filter_by_regex', None)
        self.enable_count_measurement = kwargs.get('enable_count_measurement', False)
        self.force_insert_even_csv_no_update = kwargs.get('force_insert_even_csv_no_update', True)
        self.force_string_columns = kwargs.get('force_string_columns', None)
        self.force_int_columns = kwargs.get('force_int_columns', None)
        self.force_float_columns = kwargs.get('force_float_columns', None)
        self.http_schema = kwargs.get('http_schema', 'http')
        self.org_name = kwargs.get('org_name', 'my-org')
        self.bucket_name = kwargs.get('bucket_name', 'my-bucket')
        self.token = kwargs.get('token', None)
        self.unique = kwargs.get('unique', False)
        self.csv_charset = kwargs.get('csv_charset', None)

        # Validate conf
        base_object = BaseObject()
        base_object.validate_str(self.csv_file, target_name='csv_file')
        base_object.validate_str(self.db_name, target_name='db_name')
        base_object.validate_str(self.db_measurement, target_name='db_measurement')
        base_object.validate_str(self.time_format, target_name='time_format')
        base_object.validate_str(self.delimiter, target_name='delimiter')
        base_object.validate_str(self.lineterminator, target_name='lineterminater')
        base_object.validate_str(self.time_zone, target_name='time_zone')
        self.tag_columns = base_object.str_to_list(self.tag_columns)
        self.field_columns = base_object.str_to_list(self.field_columns)
        self.limit_string_length_columns = base_object.str_to_list(self.limit_string_length_columns)
        self.match_columns = base_object.str_to_list(self.match_columns)
        self.match_by_string = base_object.str_to_list(self.match_by_string)
        self.match_by_regex = base_object.str_to_list(self.match_by_regex, lower=False)
        self.filter_columns = base_object.str_to_list(self.filter_columns)
        self.filter_by_string = base_object.str_to_list(self.filter_by_string)
        self.filter_by_regex = base_object.str_to_list(self.filter_by_regex)
        self.drop_database = self.__validate_bool_string(self.drop_database)
        self.drop_measurement = self.__validate_bool_string(self.drop_measurement)
        self.enable_count_measurement = self.__validate_bool_string(self.enable_count_measurement)
        self.force_insert_even_csv_no_update = self.__validate_bool_string(self.force_insert_even_csv_no_update)
        self.force_string_columns = base_object.str_to_list(self.force_string_columns)
        self.force_int_columns = base_object.str_to_list(self.force_int_columns)
        self.force_float_columns = base_object.str_to_list(self.force_float_columns)
        base_object.validate_str(self.http_schema, target_name='http_schema')
        base_object.validate_str(self.org_name, target_name='org_name')
        base_object.validate_str(self.bucket_name, target_name='bucket_name')
        base_object.validate_str(self.token, target_name='token')
        self.unique = self.__validate_bool_string(self.unique)
        base_object.validate_str(self.csv_charset, target_name='csv_charset')

        # Fields should not duplicate in force_string_columns, force_int_columns, force_float_columns
        all_force_columns = self.force_string_columns + self.force_int_columns + self.force_float_columns
        duplicates = [item for item, count in collections.Counter(all_force_columns).items() if count > 1]
        if duplicates:
            error_message = 'Error: Find duplicate items: {0} in: \n' \
                            '       force_string_columns: {1} \n' \
                            '       force_int_columns: {2} \n' \
                            '       force_float_columns: {3}'.format(duplicates,
                                                                     self.force_string_columns,
                                                                     self.force_int_columns,
                                                                     self.force_float_columns)
            sys.exit(error_message)

        # Fields should not duplicate in match_columns, filter_columns
        all_force_columns = self.match_columns + self.filter_columns
        duplicates = [item for item, count in collections.Counter(all_force_columns).items() if count > 1]
        if duplicates:
            error_message = 'Error: Find duplicate items: {0} in: \n' \
                            '       match_columns: {1} \n' \
                            '       filter_columns: {2} '.format(duplicates,
                                                                 self.match_columns,
                                                                 self.filter_columns)
            sys.exit(error_message)

        # Validate: batch_size
        try:
            self.batch_size = int(self.batch_size)
        except ValueError:
            error_message = 'Error: The batch_size should be int, current is: {0}'.format(self.batch_size)
            sys.exit(error_message)

        # Validate: limit_length
        try:
            self.limit_length = int(self.limit_length)
        except ValueError:
            error_message = 'Error: The limit_length should be int, current is: {0}'.format(self.limit_length)
            sys.exit(error_message)

        # Validate csv
        current_dir = os.path.curdir
        csv_file = os.path.join(current_dir, self.csv_file)
        csv_file_exists = os.path.exists(csv_file)
        if csv_file_exists is False:
            error_message = 'Error: CSV file not found, exiting...'
            sys.exit(error_message)

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
