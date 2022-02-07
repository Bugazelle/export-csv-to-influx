from requests import ConnectionError
import requests
import sys


class InfluxObject(object):
    """InfluxObject"""

    def __init__(self, db_server_name='127.0.0.1:8086', db_user='admin', db_password='admin', http_schema='http'):
        self.db_server_name = db_server_name.lower()
        self.db_user = db_user
        self.db_password = db_password
        self.host = self.db_server_name[0:self.db_server_name.rfind(':')]
        self.host = self.host.lower().replace('http://', '').replace('https://', '')
        try:
            self.port = int(self.db_server_name[self.db_server_name.rfind(':') + 1:])
        except ValueError:
            if 'https' in self.db_server_name.lower():
                self.port = 433
            else:
                self.port = 80
        self.http_schema = http_schema
        if 'https' in self.db_server_name.lower():
            self.http_schema = 'https'
        self.influxdb_url = '{http_schema}://{host}:{port}/'.format(http_schema=self.http_schema,
                                                                    host=self.host,
                                                                    port=self.port)

    def get_influxdb_version(self):
        """Function: get_influxdb_version

        :return influxdb version
        """
        try:
            req_influxdb = requests.get(self.influxdb_url, verify=False)
            response_headers = req_influxdb.headers
            influxdb_version = response_headers['X-Influxdb-Version']
            return influxdb_version
        except ConnectionError:
            sys.exit('Error: Failed to connect the influx {influxdb_url}'.format(influxdb_url=self.influxdb_url))
        except KeyError:
            sys.exit('Error: This is not a valid influx {influxdb_url}'.format(influxdb_url=self.influxdb_url))
        except requests.exceptions.SSLError:
            sys.exit('Error: SSL error when connecting influx {influxdb_url}'.format(influxdb_url=self.influxdb_url))

    def connect_influx_db(self, db_name):
        """Function: connect_influx_db

        :param db_name: the influx db name
        :return client
        """

        influxdb_version = self.get_influxdb_version()
        if influxdb_version.startswith('0') or influxdb_version.startswith('1'):
            # influxdb 0.x, 1.x
            from influxdb import InfluxDBClient
            try:
                client = InfluxDBClient(self.host, self.port, self.db_user, self.db_password, db_name, timeout=120)
                client.get_list_database()
                return client
            except ConnectionError:
                sys.exit('Error: Failed to connect the influx {host}:{port}'.format(host=self.host, port=self.port))
        else:
            # influxdb 2.x
            from influxdb_client import InfluxDBClient
            try:
                client = InfluxDBClient(self.host, self.port, self.db_user, self.db_password, db_name, timeout=120)
                client = InfluxDBClient(url=self.influxdb_url, token="my-token", org="my-org")
                client.get_list_database()
                return client
            except ConnectionError:
                sys.exit('Error: Failed to connect the influx {host}:{port}'.format(host=self.host, port=self.port))

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
