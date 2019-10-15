import sys

class BaseObject(object):
    """BaseObject"""

    def __init__(self):
        self.strip_chars = ' \r\n\t/"\',\\'

    @staticmethod
    def convert_boole(target):
        target = str(target).lower()
        if target != 'true' and target != 'false':
            print('Error: The expected input for {0} should be: True or False'.format(target))
            exit(1)
        if target == 'true':
            target = True
        else:
            target = False

        return target

    def validate_str(self, target, ignore_exception=False):
        """Function: validate_string

        :param target: the string
        :param ignore_exception: the True or False
        """

        get_type = type(target)
        ignore_exception = self.convert_boole(ignore_exception)
        try:
            string_type = get_type is str or get_type is unicode
        except NameError:
            string_type = get_type is str
        if not string_type and ignore_exception is False:
            print('Error: The {0} is not string type. Please check.'.format(target))
            sys.exit(1)

        return string_type

    def str_to_list(self, string, delimiter=',', lower=False):
        """Function: str_to_list

        :param string: the string
        :param delimiter: the delimiter for list (default comma)
        :param lower: lower the string (default False)
        :return
        """

        get_type = type(string)
        error_message = 'Error: The string should be list or string, use comma to separate. ' \
                        'Current is: type-{0}, {1}'.format(get_type, string)

        # Process if Value Error
        try:
            bool(string)
        except ValueError:
            print(error_message)
            sys.exit(1)

        # Process the type
        list_tuple_type = get_type is list or get_type is tuple
        str_unicode_type = self.validate_str(string, True)
        if list_tuple_type:
            if lower:
                li = [str(item).strip(self.strip_chars).lower() for item in string]
            else:
                li = [str(item).strip(self.strip_chars) for item in string]
        elif str_unicode_type:
            li = string.strip(self.strip_chars).split(delimiter)
            if lower:
                li = [item.strip(self.strip_chars).lower() for item in li]
            else:
                li = [item.strip(self.strip_chars) for item in li]
        elif not string:
            li = list()
        else:
            print(error_message)
            sys.exit(1)
        return li
