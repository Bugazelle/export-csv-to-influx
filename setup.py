from setuptools import setup, find_packages
import sys
import os
import re
import io

version_info = sys.version_info

CURDIR = os.path.dirname(os.path.abspath(__file__))
url = 'https://github.com/Bugazelle/export-csv-to-influx'

with io.open(os.path.join(CURDIR, 'src', 'ExportCsvToInflux', '__version__.py'), encoding='utf-8') as f:
    VERSION = re.search("__version__ = '(.*)'", f.read()).group(1)
    download_url = '{0}/archive/v{1}.tar.gz'.format(url, VERSION)


def readme():
    with io.open('README.md', encoding='utf-8') as f:
        long_description = f.read()
        return long_description


setup(
    name='ExportCsvToInflux',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    version=VERSION,
    license='bsd-3-clause-clear',
    description='Export',
    zip_safe=False,
    include_package_data=True,
    long_description=readme(),
    long_description_content_type='text/markdown',
    author='Bugazelle',
    author_email='463407426@qq.com',
    keywords=['python', 'csv', 'influx'],
    install_requires=[
        'influxdb>=5.3.1',
        'influxdb-client[ciso]>=1.25.0' if version_info >= (3, 6) else '',
        'python-dateutil>=2.8.0',
        'chardet>=4.0.0'
    ],
    download_url=download_url,
    url=url,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.9',
    ],
    entry_points={
        'console_scripts': [
            'export_csv_to_influx = ExportCsvToInflux.command_object:export_csv_to_influx',
        ],
    },
)


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


if version_info <= (3, 5):
    print(bcolors.WARNING
          + 'WARNING: Your Python version is {0}.{1} < 3.6, '
            'which only supports the influxDB 0.x, 1.x.'.format(version_info[0], version_info[1])
          + bcolors.ENDC)
    print(bcolors.WARNING
          + 'WARNING: If you would like the lib supports influxDB2.x, please upgrade Python >= 3.6.'
          + bcolors.ENDC)
    print(bcolors.WARNING
          + 'WARNING: Alternatively, influxdb 2.x has build-in csv write feature, '
            'it is more powerful: https://docs.influxdata.com/influxdb/v2.1/write-data/developer-tools/csv/'
          + bcolors.ENDC)
