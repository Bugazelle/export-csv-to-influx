from influxdb import InfluxDBClient


class InfluxObject(object):
    """InfluxObject"""

    def __init__(self, db_server_name='127.0.0.1:8086', db_user='admin', db_password='admin'):
        self.db_server_name = db_server_name
        self.db_user = db_user
        self.db_password = db_password

    def connect_influx_db(self, db_name):
        """Function: connect_influx_db

        :param db_name: the influx db name
        :return client
        """
        host = self.db_server_name[0:self.db_server_name.rfind(':')]
        try:
            port = int(self.db_server_name[self.db_server_name.rfind(':') + 1:])
        except ValueError:
            if 'https' in self.db_server_name.lower():
                port = 433
            else:
                port = 80
        client = InfluxDBClient(host, port, self.db_user, self.db_password, db_name, timeout=120)

        return client

    def create_influx_db_if_not_exists(self, db_name):
        """Function: create_influx_db_if_not_exists

        :param db_name: the influx db name
        :return client
        """

        # Connect DB
        client = self.connect_influx_db(db_name)

        # Check & Create DB
        databases = client.get_list_database()
        db_found = False
        for db in databases:
            if db['name'] == db_name:
                db_found = True
                print('Info: Database {0} already exists'.format(db_name))
                break
        if db_found is False:
            print('Info: Database {0} not found, trying to create it'.format(db_name))
            client.create_database(db_name)
            print('Info: Database {0} created'.format(db_name))
        client.switch_database(db_name)

        return client

    def drop_database(self, db_name):
        """Function: drop_database

                :param db_name: the influx db name
                :return client
                """

        # Connect DB
        client = self.connect_influx_db(db_name)

        # Drop DB
        client.drop_database(db_name)

        return client

    def drop_measurement(self, db_name, measurement):
        """Function: drop_measurement

        :param db_name: the influx db name
        :param measurement: the measurement
        :return client
        """

        # Connect DB
        client = self.connect_influx_db(db_name)

        # Drop Measurement
        client.drop_measurement(measurement)

        return client
