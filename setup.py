from setuptools import setup, find_packages
import os
import re

CURDIR = os.path.dirname(os.path.abspath(__file__))
url = 'https://github.com/Bugazelle/export-csv-to-inlfux'

with open(os.path.join(CURDIR, 'requirements.txt')) as f:
    REQUIRES = f.read().splitlines()

with open(os.path.join(CURDIR, 'src', 'ExportCsvToInflux', '__version__.py')) as f:
    VERSION = re.search("__version__ = '(.*)'", f.read()).group(1)
    download_url = '{0}/archive/v{1}.tar.gz'.format(url, VERSION)

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()
    index = long_description.find('''```\n\n> **Note''')
    new_long_description = long_description[:index].replace('```bash', '')\
        .replace('## Install', '**Install**')\
        .replace('## Features', '**Features**')\
        .replace('## Command Arguments', '**Command Arguments**')
    suffix = '\nFor more info, please refer to the {0}'.format(url)
    long_description = new_long_description + suffix

setup(
    name='ExportCsvToInflux',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    version=VERSION,
    license='bsd-3-clause-clear',
    description='Export',
    zip_safe=False,
    include_package_data=True,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Bugazelle',
    author_email='463407426@qq.com',
    keywords=['python', 'csv', 'influx'],
    install_requires=REQUIRES,
    download_url=download_url,
    url=url,
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ),
    entry_points={
        'console_scripts': [
            'export_csv_to_influx = ExportCsvToInflux.exporter_object:export_csv_to_influx',
        ],
    },
)
