from requests import ConnectionError
import requests
import datetime
import sys


class InfluxObject(object):
    """InfluxObject"""

    def __init__(self,
                 db_server_name='127.0.0.1:8086',
                 db_user='admin',
                 db_password='admin',
                 http_schema='http',
                 token=None):
        self.db_server_name = db_server_name.lower()
        self.db_user = db_user
        self.db_password = db_password
        self.token = token
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

        self.influxdb_version = self.get_influxdb_version()

    @property
    def influxdb_client_type(self):
        """Function: influxdb_client_type

        :return influxdb client object
        """
        if self.influxdb_version.startswith('0') or self.influxdb_version.startswith('1'):
            from influxdb.client import InfluxDBClient
        else:
            from influxdb_client.client.influxdb_client import InfluxDBClient
        return InfluxDBClient

    @property
    def influxdb_client_error(self):
        """Function: influxdb_client_error

        :return influxdb client error
        """
        if self.influxdb_version.startswith('0') or self.influxdb_version.startswith('1'):
            from influxdb.exceptions import InfluxDBClientError as InfluxDBError
        else:
            from influxdb_client.client.exceptions import InfluxDBError
        return InfluxDBError

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

    def connect_influx_db(self, db_name='', org_name=''):
        """Function: connect_influx_db

        :param db_name: for influx 0.x 1.x, the influx db name
        :param org_name: for influx 2.x, the influx org name
        :return client
        """

        if self.influxdb_version.startswith('0') or self.influxdb_version.startswith('1'):
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
                client = InfluxDBClient(url=self.influxdb_url, token=self.token, org=org_name)
                client.buckets_api().find_buckets()
                return client
            except (ConnectionError, Exception):
                sys.exit('Error: Failed to connect the influx, please check the provided server name - {0}, '
                         'org name - {1} and token - {2}'.format(self.db_server_name, org_name, self.token))

    def create_influx_org_if_not_exists(self, org_name, client=None):
        """Function: create_influx_org_if_not_exists

        :param org_name: for influx 2.x, the org name
        :param client: the influxdb client
        :return client
        """

        # Connect DB
        if type(client) is not self.influxdb_client_type:
            client = self.connect_influx_db(org_name=org_name)

        if self.influxdb_version.startswith('2'):
            # influxdb 2.x
            try:
                orgs = client.organizations_api().find_organizations(org=org_name)
            except self.influxdb_client_error:
                orgs = None
            if orgs:
                print('Info: Org {0} already exists'.format(org_name))
            else:
                print('Info: Org {0} not found, trying to create it'.format(org_name))
                try:
                    client.organizations_api().create_organization(name=org_name)
                    print('Info: Org {0} created'.format(org_name))
                except self.influxdb_client_error as e:
                    sys.exit('Error: Failed to create org with the error {0}'.format(e))
        else:
            # influxdb 0.x, 1.x
            print('Waring: The influx {0} does not have org, skip creating'.format(self.influxdb_version))

        return client

    def drop_org(self, org_name, client=None):
        """Function: drop_org

        :param org_name: for influx 2.x, the org name
        :param client: the influxdb client
        :return client
        """

        # Connect DB
        if type(client) is not self.influxdb_client_type:
            client = self.connect_influx_db(org_name=org_name)

        # Drop DB or Bucket
        if self.influxdb_version.startswith('2') :
            # influxdb 2.0
            try:
                orgs = client.organizations_api().find_organizations(org=org_name)
            except self.influxdb_client_error:
                orgs = None
            if orgs:
                org_id = orgs[0].id
                client.organizations_api().delete_organization(org_id)
                print('Info: Org {0} dropped successfully'.format(org_name))
            else:
                sys.exit("Error: You don't have access to drop the org, or the org does not exist")
        else:
            # influxdb 0.x, 1.x
            print('Waring: The influx {0} does not have org, skip dropping'.format(self.influxdb_version))

        return client

    def create_influx_bucket_if_not_exists(self, org_name, bucket_name, client=None):
        """Function: create_influx_bucket_if_not_exists

        :param org_name: for influx 2.x, the org name
        :param bucket_name: for influx 2.x, the bucket name
        :param client: the influxdb client
        :return client
        """

        # Connect DB
        if type(client) is not self.influxdb_client_type:
            client = self.connect_influx_db(org_name=org_name)

        if self.influxdb_version.startswith('2'):
            # influxdb 2.x
            try:
                bucket = client.buckets_api().find_buckets(org=org_name, name=bucket_name)
            except self.influxdb_client_error:
                bucket = None
            if bucket:
                print('Info: Bucket {0} already exists'.format(bucket_name))
            else:
                print('Info: Bucket {0} not found, trying to create it'.format(bucket_name))
                try:
                    client.buckets_api().create_bucket(bucket_name=bucket_name, org=org_name)
                    print('Info: Bucket {0} created'.format(bucket_name))
                except self.influxdb_client_error as e:
                    sys.exit('Error: Failed to create bucket with the error {0}'.format(e))
        else:
            # influxdb 0.x, 1.x
            print('Waring: The influx {0} does not have bucket, skip creating'.format(self.influxdb_version))

        return client

    def drop_bucket(self, org_name, bucket_name, client=None):
        """Function: drop_org

        :param org_name: for influx 2.x, the org name
        :param bucket_name: for influx 2.x, the bucket name
        :param client: the influxdb client
        :return client
        """

        # Connect DB
        if type(client) is not self.influxdb_client_type:
            client = self.connect_influx_db(org_name=org_name)

        # Drop DB or Bucket
        if self.influxdb_version.startswith('2'):
            # influxdb 2.0
            try:
                bucket = client.buckets_api().find_buckets(org=org_name, name=bucket_name)
                bucket = bucket.buckets[0]
            except self.influxdb_client_error:
                bucket = None
            if bucket:
                client.buckets_api().delete_bucket(bucket)
                print('Info: Bucket {0} dropped successfully'.format(bucket_name))
            else:
                sys.exit("Error: You don't have access to drop the bucket, or the bucket does not exist")
        else:
            # influxdb 0.x, 1.x
            print('Waring: The influx {0} does not have org, skip dropping'.format(self.influxdb_version))

        return client

    def create_influx_db_if_not_exists(self, db_name, client=None):
        """Function: create_influx_db_if_not_exists

        :param db_name: for influx 0.x, 1.x, the influx db name
        :param client: the influxdb client
        :return client
        """

        # Connect DB
        if type(client) is not self.influxdb_client_type:
            client = self.connect_influx_db(db_name=db_name)

        if self.influxdb_version.startswith('0') or self.influxdb_version.startswith('1'):
            # influxdb 0.x, 1.x
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
        else:
            # influxdb 2.x
            print('Warning: The influx is {0}, skip database creating'.format(self.influxdb_version))

        return client

    def drop_database(self, db_name, client=None):
        """Function: drop_database_or_org

        :param db_name: for influx 0.x, 1.x, the influx db name
        :param client: the influxdb client
        :return client
        """

        # Connect DB
        if type(client) is not self.influxdb_client_type:
            client = self.connect_influx_db(db_name=db_name)

        # Drop DB or Bucket
        if self.influxdb_version.startswith('0') or self.influxdb_version.startswith('1'):
            # influxdb 0.x, 1.x
            client.drop_database(db_name)
            print('Info: Database {0} dropped successfully'.format(db_name))
        else:
            # influxdb 2.x
            print('Warning: The influx is {0}, skip database dropping'.format(self.influxdb_version))

        return client

    def drop_measurement(self, db_name, measurement, bucket='', org='', client=None):
        """Function: drop_measurement_or_bucket

        :param db_name: for influx 0.x, 1.x, he influx db name
        :param measurement: for influx 0.x, 1.x, the measurement
        :param client: the influxdb client
        :param bucket: for influx2.x, the bucket name or id
        :param org: for influx2.x, the org name or id
        :return client
        """

        # Connect DB
        if type(client) is not self.influxdb_client_type:
            client = self.connect_influx_db(db_name=db_name)

        # Drop Measurement
        if self.influxdb_version.startswith('0') or self.influxdb_version.startswith('1'):
            # influxdb 0.x, 1.x
            client.drop_measurement(measurement)
        else:
            # influxdb 2.x
            start = "1970-01-01T00:00:00Z"
            stop = "{0}Z".format(datetime.datetime.now().replace(microsecond=0).isoformat())
            client.delete_api().delete(start,
                                       stop,
                                       '_measurement="{0}"'.format(measurement),
                                       bucket=bucket,
                                       org=org)
        print('Info: Measurement {0} dropped successfully'.format(measurement))

        return client
