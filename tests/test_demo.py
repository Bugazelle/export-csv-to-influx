from src.ExportCsvToInflux.csv_object import CSVObject
from mock import patch
import pytest


@pytest.fixture
def csv_file(tmp_path):
    directory = tmp_path
    temp_csv_path = directory.joinpath('temp.csv')
    data = '''header1,header2
        a,b
        c,d
    '''
    temp_csv_path.write_text(data)

    return temp_csv_path


@patch('src.ExportCsvToInflux.csv_object.CSVObject.valid_file_exist')
def test_get_csv_header(mock_file_exist, csv_file):
    csv_object = CSVObject()
    headers = csv_object.get_csv_header(csv_file)
    assert headers == ['header1', 'header2']
