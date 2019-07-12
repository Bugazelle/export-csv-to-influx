class BaseObject(object):
    """BaseObject"""

    def __init__(self):
        self.strip_chars = ' \r\n\t/"\',\\'

    def str_to_list(self, string, delimiter=',', lower=False):
        """Function: str_to_list

        :param string: the string
        :param delimiter: the delimiter for list (default comma)
        :param lower: lower the string (default False)
        :return
        """

        string_type = type(string)
        if string_type is list or string_type is tuple:
            if lower:
                li = [str(item).strip(self.strip_chars).lower() for item in string]
            else:
                li = [str(item).strip(self.strip_chars) for item in string]
        elif string_type is str or string_type is unicode:
            li = string.strip(self.strip_chars).split(delimiter)
            if lower:
                li = [item.strip(self.strip_chars).lower() for item in li]
            else:
                li = [item.strip(self.strip_chars) for item in li]
        elif bool(string) is False:
            li = list()
        else:
            raise Exception('Error: The string should be list or string, use comma to separate. '
                            'Current is: type-{0}, {1}'.format(string_type, string))
        return li
