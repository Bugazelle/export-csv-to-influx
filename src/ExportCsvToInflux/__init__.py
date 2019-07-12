from .influx_object import InfluxObject
from .csv_object import CSVObject
from .exporter_object import ExporterObject
from .base_object import BaseObject
from .__version__ import __version__

_version_ = __version__


class ExportCsvToInflux(InfluxObject,
                        CSVObject,
                        ExporterObject,
                        BaseObject,):
    """ExportCsvToInflux is library to export csv data into influx db"""

    def __init__(self):
        super(ExportCsvToInflux, self).__init__()
