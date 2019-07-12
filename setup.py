from setuptools import setup, find_packages
import os
import re

CURDIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(CURDIR, 'requirements.txt')) as f:
    REQUIRES = f.read().splitlines()

with open(os.path.join(CURDIR, 'src', 'ExportCsvToInflux', '__version__.py')) as f:
    VERSION = re.search("__version__ = '(.*)'", f.read()).group(1)

setup(
    name='ExportCsvToInflux',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    version=VERSION,
    zip_safe=False,
    include_package_data=True,
    description='ExportCsvToInflux: A Solution to export csv to influx db',
    author='Bugazelle',
    author_email='463407426@qq.com',
    keywords=['python', 'csv', 'influx'],
    install_requires=REQUIRES,
    download_url='https://github.com/Bugazelle/export-csv-to-inlfux/archive/v{0}.tar.gz'.format(VERSION),
    url='https://github.com/Bugazelle/export-csv-to-inlfux',
    classifiers=(
        'Development Status :: Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 237',
    ),
    entry_points={
        'console_scripts': [
            'export_csv_to_influx = ExportCsvToInflux.exporter_object:export_csv_to_influx',
        ],
    },
)
