from setuptools import setup, find_packages
import os
import re
import io

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
        'influxdb>=5.2.2',
        'python-dateutil>=2.8.0'
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
    ],
    entry_points={
        'console_scripts': [
            'export_csv_to_influx = ExportCsvToInflux.exporter_object:export_csv_to_influx',
        ],
    },
)
