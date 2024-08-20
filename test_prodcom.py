import unittest
from unittest.mock import patch, MagicMock, call
from collect_tasks.prodcom_collect import get_datasets_info, prettify_data, save_prettified_xml_to_zip, get_metadata, get_data_version, get_data_columns, get_data_description, get_data, prodcom_collect, Config
import json
import pandas as pd
import io
from xml.dom.minidom import parseString
import zipfile
import os
from pathlib import Path
import requests
import xml.etree.ElementTree as ET


###############################################################
#                     Test get_datasets_info                  #
###############################################################

class TestGetDatasetsInfo(unittest.TestCase):
    @patch('collect_tasks.prodcom_collect.requests.get')
    @patch('collect_tasks.prodcom_collect.prettify_data')
    @patch('builtins.open', new_callable=unittest.mock.mock_open)
    @patch('collect_tasks.prodcom_collect.xml2csv_metadata')
    def test_successful_execution(self, mock_xml2csv_metadata, mock_open, mock_prettify_data, mock_requests_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.content = b'<xml>data</xml>'
        mock_requests_get.return_value = mock_response
        
        # Mock prettified data
        mock_prettify_data.return_value = '<xml>pretty data</xml>'
        
        # Define the directory to save files
        save_dir = 'mock_save_dir'
        
        # Call the function
        get_datasets_info('dataflow', save_dir)
        
        # Check if requests.get was called correctly
        mock_requests_get.assert_called_once_with('https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/dataflow/ESTAT/all', params={'lang': None, 'detail': 'allstubs', 'completestub': 'true'})
        
        # Check if prettify_data was called correctly
        mock_prettify_data.assert_called_once_with(b'<xml>data</xml>', '.xml')
        
        # Check if file was opened and written to correctly
        mock_open.assert_called_once_with(f'{save_dir}/dataflow.xml', 'w', encoding='utf-8')
        mock_open().write.assert_called_once_with('<xml>pretty data</xml>')
        
        # Check if xml2csv_metadata was called
        mock_xml2csv_metadata.assert_called_once_with(save_dir, save_dir, 'dataflow')
    
    @patch('collect_tasks.prodcom_collect.requests.get')
    @patch('collect_tasks.prodcom_collect.prettify_data')
    @patch('builtins.open', new_callable=unittest.mock.mock_open)
    @patch('collect_tasks.prodcom_collect.xml2csv_metadata')
    def test_http_request_failure(self, mock_xml2csv_metadata, mock_open, mock_prettify_data, mock_requests_get):
        # Mock an HTTP request failure
        mock_requests_get.side_effect = requests.RequestException("HTTP request failed")
        
        # Define the directory to save files
        save_dir = 'mock_save_dir'
        
        # Call the function and assert it raises an exception
        with self.assertRaises(requests.RequestException):
            get_datasets_info('dataflow', save_dir)
        
        # Ensure file write was not attempted
        mock_open.assert_not_called()
        
        # Ensure xml2csv_metadata was not called
        mock_xml2csv_metadata.assert_not_called()
    
    @patch('collect_tasks.prodcom_collect.requests.get')
    @patch('collect_tasks.prodcom_collect.prettify_data')
    @patch('builtins.open', new_callable=unittest.mock.mock_open)
    @patch('collect_tasks.prodcom_collect.xml2csv_metadata')
    def test_file_save_failure(self, mock_xml2csv_metadata, mock_open, mock_prettify_data, mock_requests_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.content = b'<xml>data</xml>'
        mock_requests_get.return_value = mock_response
        
        # Mock prettified data
        mock_prettify_data.return_value = '<xml>pretty data</xml>'
        
        # Simulate file open failure
        mock_open.side_effect = IOError("Failed to open file")
        
        # Define the directory to save files
        save_dir = 'mock_save_dir'
        
        # Call the function
        get_datasets_info('dataflow', save_dir)
        
        # Ensure xml2csv_metadata was not called
        mock_xml2csv_metadata.assert_not_called()
        
        # Check if file was attempted to be opened
        mock_open.assert_called_once_with(f'{save_dir}/dataflow.xml', 'w', encoding='utf-8')
    
    

###############################################################
#                     Test prettify_data                      #
###############################################################

def normalize_line_endings(text):
    return text.replace('\r\n', '\n')

class TestPrettifyData(unittest.TestCase):

    def test_prettify_json(self):
        data = '{"name": "John", "age": 30, "city": "New York"}'
        expected_output = json.dumps(json.loads(data), indent=4)
        self.assertEqual(prettify_data(data, ".json"), expected_output)


    def test_prettify_xml(self):
        data = '<root><name>John</name><age>30</age><city>New York</city></root>'
        expected_output = '<?xml version="1.0" ?>\n<root>\n  <name>John</name>\n  <age>30</age>\n  <city>New York</city>\n</root>\n'
        self.assertEqual(prettify_data(data, ".xml"), expected_output)


    def test_prettify_csv(self):
        data = "name,age,city\nJohn,30,New York"
        expected_output = "name,age,city\nJohn,30,New York\n"
        self.assertEqual(normalize_line_endings(prettify_data(data, ".csv")), normalize_line_endings(expected_output))

    def test_prettify_tsv(self):
        data = "name\tage\tcity\nJohn\t30\tNew York"
        expected_output = "name\tage\tcity\nJohn\t30\tNew York\n"
        self.assertEqual(normalize_line_endings(prettify_data(data, ".tsv")), normalize_line_endings(expected_output))

    def test_prettify_unknown_extension(self):
        data = "Just some text"
        self.assertEqual(prettify_data(data, ".txt"), data)

    def test_prettify_malformed_json(self):
        data = '{"name": "John", "age": 30, "city": "New York"'
        with self.assertRaises(json.JSONDecodeError):
            prettify_data(data, ".json")

    def test_prettify_malformed_xml(self):
        data = '<root><name>John</name><age>30</age><city>New York</city>'
        # Expecting the original data to be returned since prettifying should fail
        self.assertEqual(prettify_data(data, ".xml"), data)

###############################################################
#               Test save_prettified_xml_to_zip               #
###############################################################

class TestSavePrettifiedXmlToZip(unittest.TestCase):

    @patch('collect_tasks.prodcom_collect.ZipFile')  # Mock ZipFile
    @patch('collect_tasks.prodcom_collect.prettify_data')  # Mock prettify_data
    @patch('collect_tasks.prodcom_collect.logger')  # Mock logger
    def test_save_prettified_xml_to_zip_success(self, mock_logger, mock_prettify_data, mock_zipfile):
        xml_data = '<root><name>John</name><age>30</age><city>New York</city></root>'
        output_zip_path = 'test.zip'
        xml_filename = 'test.xml'
        prettified_xml = '<?xml version="1.0" ?>\n<root>\n  <name>John</name>\n  <age>30</age>\n  <city>New York</city>\n</root>\n'
        mock_prettify_data.return_value = prettified_xml

        # Mock ZipFile context manager
        mock_zip_file = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Call the function
        save_prettified_xml_to_zip(xml_data, output_zip_path, xml_filename)

        # Check if prettify_data was called with the correct arguments
        mock_prettify_data.assert_called_once_with(xml_data, '.xml')

        # Check if ZipFile was used correctly
        mock_zipfile.assert_called_once_with(output_zip_path, 'w', zipfile.ZIP_DEFLATED)
        mock_zip_file.writestr.assert_called_once_with(xml_filename, prettified_xml)

        # Check if logger.info was called
        mock_logger.info.assert_called_once_with(f"Prettified XML saved to {output_zip_path} as {xml_filename}")

    @patch('collect_tasks.prodcom_collect.ZipFile')  # Mock ZipFile
    @patch('collect_tasks.prodcom_collect.prettify_data')  # Mock prettify_data
    @patch('collect_tasks.prodcom_collect.logger')  # Mock logger
    def test_save_prettified_xml_to_zip_error_handling(self, mock_logger, mock_prettify_data, mock_zipfile):
        xml_data = '<root><name>John</name><age>30</age><city>New York</city></root>'
        output_zip_path = 'test.zip'
        xml_filename = 'test.xml'
        mock_prettify_data.return_value = '<?xml version="1.0" ?>\n<root>\n  <name>John</name>\n  <age>30</age>\n  <city>New York</city>\n</root>\n'
        
        # Simulate an exception when writing to the zip file
        mock_zip_file = MagicMock()
        mock_zip_file.writestr.side_effect = Exception("Write failed")
        mock_zipfile.return_value.__enter__.return_value = mock_zip_file

        # Call the function and expect it to handle the exception
        save_prettified_xml_to_zip(xml_data, output_zip_path, xml_filename)

        # Check if logger.error was called
        mock_logger.error.assert_called_once_with("Failed to save prettified XML to zip: Write failed")

###############################################################
#                      Test get_metadata                      #
###############################################################

class TestGetMetadata(unittest.TestCase):

    @patch('collect_tasks.prodcom_collect.get_datasets_info')  # Mock get_datasets_info
    @patch('collect_tasks.prodcom_collect.pd.read_csv')  # Mock pd.read_csv
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    @patch('collect_tasks.prodcom_collect.requests.get')  # Mock requests.get
    @patch('collect_tasks.prodcom_collect.save_prettified_xml_to_zip')  # Mock save_prettified_xml_to_zip
    @patch('collect_tasks.prodcom_collect.logger')  # Mock logger
    def test_get_metadata_success(self, mock_logger, mock_save_prettified_xml_to_zip, mock_requests_get, mock_os_remove, mock_read_csv, mock_get_datasets_info):
        id = '123'
        metadata_type = 'codelist'
        save_dir = '/fake/dir'
        
        # Prepare mock CSV data
        csv_data = pd.DataFrame({
            'id': ['123', '456'],
            'structureURL': ['http://example.com/123', 'http://example.com/456']
        })
        mock_read_csv.return_value = csv_data
        mock_requests_get.return_value = MagicMock(content=b'fake_xml_content')
        
        # Mock save_prettified_xml_to_zip to avoid actual file operations
        mock_save_prettified_xml_to_zip.return_value = None

        # Call the function
        get_metadata(id, metadata_type, save_dir)

        # Assertions
        mock_get_datasets_info.assert_called_once_with(metadata_type, '.')
        mock_read_csv.assert_called_once_with(f'{metadata_type}.csv')

        # Assert that os.remove is called twice with the correct arguments
        mock_os_remove.assert_has_calls([
            call(f'{metadata_type}.csv'),
            call(f'{metadata_type}.xml')
        ], any_order=False)

        mock_requests_get.assert_called_once_with('http://example.com/123', params={})
        mock_save_prettified_xml_to_zip.assert_called_once_with(
            b'fake_xml_content',
            Path(save_dir) / f"dim_{id}.zip",
            f"dim_{id}.xml"
        )
        mock_logger.info.assert_called_with(f"Successfully saved {metadata_type} for {id} to {save_dir}")


    @patch('collect_tasks.prodcom_collect.get_datasets_info')  # Mock get_datasets_info
    @patch('collect_tasks.prodcom_collect.pd.read_csv')  # Mock pd.read_csv
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    @patch('collect_tasks.prodcom_collect.requests.get')  # Mock requests.get
    @patch('collect_tasks.prodcom_collect.save_prettified_xml_to_zip')  # Mock save_prettified_xml_to_zip
    @patch('collect_tasks.prodcom_collect.logger')  # Mock logger
    def test_get_metadata_no_matching_id(self, mock_logger, mock_save_prettified_xml_to_zip, mock_requests_get, mock_os_remove, mock_read_csv, mock_get_datasets_info):
        id = '999'
        metadata_type = 'codelist'
        save_dir = '/fake/dir'
        csv_data = pd.DataFrame({
            'id': ['123', '456'],
            'structureURL': ['http://example.com/123', 'http://example.com/456']
        })
        mock_read_csv.return_value = csv_data
        mock_os_remove.return_value = None

        # Call the function
        get_metadata(id, metadata_type, save_dir)

        # Assertions
        mock_get_datasets_info.assert_called_once_with(metadata_type, '.')
        mock_read_csv.assert_called_once_with(f'{metadata_type}.csv')
        mock_os_remove.assert_has_calls([
            call(f'{metadata_type}.csv'),
            call(f'{metadata_type}.xml')
        ], any_order=False)
        mock_requests_get.assert_not_called()
        mock_save_prettified_xml_to_zip.assert_not_called()
        mock_logger.warning.assert_called_once_with(f"No {metadata_type} found for id {id} in the CSV.")

    @patch('collect_tasks.prodcom_collect.get_datasets_info')  # Mock get_datasets_info
    @patch('collect_tasks.prodcom_collect.pd.read_csv')  # Mock pd.read_csv
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    @patch('collect_tasks.prodcom_collect.requests.get')  # Mock requests.get
    @patch('collect_tasks.prodcom_collect.save_prettified_xml_to_zip')  # Mock save_prettified_xml_to_zip
    @patch('collect_tasks.prodcom_collect.logger')  # Mock logger
    def test_get_metadata_exception(self, mock_logger, mock_save_prettified_xml_to_zip, mock_requests_get, mock_os_remove, mock_read_csv, mock_get_datasets_info):
        id = '123'
        metadata_type = 'codelist'
        save_dir = '/fake/dir'
        mock_read_csv.side_effect = Exception("Read CSV failed")

        # Call the function and expect it to handle the exception
        get_metadata(id, metadata_type, save_dir)

        # Assertions
        mock_get_datasets_info.assert_called_once_with(metadata_type, '.')
        mock_read_csv.assert_called_once_with(f'{metadata_type}.csv')
        mock_os_remove.assert_not_called()
        mock_requests_get.assert_not_called()
        mock_save_prettified_xml_to_zip.assert_not_called()
        mock_logger.error.assert_called_once_with(f"An error occurred during the {metadata_type} request process: Read CSV failed")


###############################################################
#                   Test get_data_version                     #
###############################################################

class TestGetDataVersion(unittest.TestCase):

    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    @patch('collect_tasks.prodcom_collect.unzip_files')  # Mock unzip_files
    @patch('collect_tasks.prodcom_collect.ET.parse')  # Mock ET.parse
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    def test_get_data_version_success(self, mock_os_remove, mock_et_parse, mock_unzip_files, mock_get_metadata):
        dataset = 'test_dataset'

        # Create a mock XML structure
        mock_tree = MagicMock()
        mock_root = ET.Element('root')

        # Create the necessary elements to match root[0][2]
        mock_element = ET.Element('element')
        mock_sub_element1 = ET.SubElement(mock_element, 'sub_element1')
        mock_sub_element2 = ET.SubElement(mock_element, 'sub_element2')
        mock_sub_element3 = ET.SubElement(mock_element, 'sub_element3')  # This will be root[0][2]

        mock_sub_element3.text = '2023-08-10T00:00:00Z'  # Ensure this element has the expected text

        mock_root.append(mock_element)
        mock_tree.getroot.return_value = mock_root
        mock_et_parse.return_value = mock_tree

        # Setup mock objects
        mock_get_metadata.return_value = None
        mock_unzip_files.return_value = None

        # Call the function
        version = get_data_version(dataset)

        # Assertions
        self.assertEqual(version, '20230810')
        mock_get_metadata.assert_called_once_with(dataset, 'conceptscheme', '.')
        mock_unzip_files.assert_called_once_with('.')
        mock_et_parse.assert_called_once_with(f'dim_{dataset}.xml')
        mock_os_remove.assert_has_calls([
            unittest.mock.call(f'dim_{dataset}.xml'),
            unittest.mock.call(f'dim_{dataset}.zip')
        ])

    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    @patch('collect_tasks.prodcom_collect.unzip_files')  # Mock unzip_files
    @patch('collect_tasks.prodcom_collect.ET.parse')  # Mock ET.parse
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    def test_get_data_version_file_not_found(self, mock_os_remove, mock_et_parse, mock_unzip_files, mock_get_metadata):
        dataset = 'test_dataset'

        # Mock ET.parse to raise FileNotFoundError
        mock_et_parse.side_effect = FileNotFoundError

        # Call the function and expect it to raise FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            get_data_version(dataset)

        # Assertions
        mock_get_metadata.assert_called_once_with(dataset, 'conceptscheme', '.')
        mock_unzip_files.assert_called_once_with('.')
        mock_et_parse.assert_called_once_with(f'dim_{dataset}.xml')
        mock_os_remove.assert_not_called()

    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    @patch('collect_tasks.prodcom_collect.unzip_files')  # Mock unzip_files
    @patch('collect_tasks.prodcom_collect.ET.parse')  # Mock ET.parse
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    def test_get_data_version_xml_parse_error(self, mock_os_remove, mock_et_parse, mock_unzip_files, mock_get_metadata):
        dataset = 'test_dataset'

        # Setup mock objects
        mock_get_metadata.return_value = None
        mock_unzip_files.return_value = None

        # Mock ET.parse to raise ET.ParseError
        mock_et_parse.side_effect = ET.ParseError

        # Call the function and expect it to raise ET.ParseError
        with self.assertRaises(ET.ParseError):
            get_data_version(dataset)

        # Assertions
        mock_get_metadata.assert_called_once_with(dataset, 'conceptscheme', '.')
        mock_unzip_files.assert_called_once_with('.')
        mock_et_parse.assert_called_once_with(f'dim_{dataset}.xml')
        mock_os_remove.assert_not_called()

    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    @patch('collect_tasks.prodcom_collect.unzip_files')  # Mock unzip_files
    @patch('collect_tasks.prodcom_collect.ET.parse')  # Mock ET.parse
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    def test_get_data_version_exception(self, mock_os_remove, mock_et_parse, mock_unzip_files, mock_get_metadata):
        dataset = 'test_dataset'

        # Setup mock objects
        mock_get_metadata.side_effect = Exception("Metadata fetch failed")

        # Call the function and expect it to raise the Exception
        with self.assertRaises(Exception):
            get_data_version(dataset)

        # Assertions
        mock_get_metadata.assert_called_once_with(dataset, 'conceptscheme', '.')
        mock_unzip_files.assert_not_called()
        mock_et_parse.assert_not_called()
        mock_os_remove.assert_not_called()


###############################################################
#                   Test get_data_columns                     #
###############################################################

class TestGetDataColumns(unittest.TestCase):

    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    @patch('collect_tasks.prodcom_collect.unzip_files')  # Mock unzip_files
    @patch('collect_tasks.prodcom_collect.ET.parse')  # Mock ET.parse
    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    def test_get_data_columns_success(self, mock_os_remove, mock_et_parse, mock_unzip_files, mock_get_metadata):
        dataset = 'test_dataset'
        # Namespace dictionary to match the function's use
        NS = {"m": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
              "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
              "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
              "xml": "http://www.w3.org/XML/1998/namespace"}

        # Create a mock XML structure with the correct namespaces
        mock_tree = MagicMock()
        mock_root = ET.Element('root')

        # Create `Enumeration` elements with the namespace
        enumeration_element = ET.Element('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Enumeration')
        item_element = ET.SubElement(enumeration_element, 'Item')
        item_element.set('id', 'COLUMN_ID_1')  # First ID

        enumeration_element2 = ET.Element('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Enumeration')
        item_element2 = ET.SubElement(enumeration_element2, 'Item')
        item_element2.set('id', 'COLUMN_ID_2')  # Second ID

        # Append to root
        mock_root.append(enumeration_element)
        mock_root.append(enumeration_element2)

        mock_tree.getroot.return_value = mock_root
        mock_et_parse.return_value = mock_tree

        # Setup mock objects
        mock_get_metadata.return_value = None
        mock_unzip_files.return_value = None

        # Call the function
        columns = get_data_columns(dataset)

        # Assertions
        self.assertEqual(columns, ['COLUMN_ID_1', 'COLUMN_ID_2'])  # Expect the IDs to be in the list
        mock_get_metadata.assert_called_once_with(dataset, 'conceptscheme', '.')
        mock_unzip_files.assert_called_once_with('.')
        mock_et_parse.assert_called_once_with(f'dim_{dataset}.xml')
        mock_os_remove.assert_has_calls([
            unittest.mock.call(f'dim_{dataset}.xml'),
            unittest.mock.call(f'dim_{dataset}.zip')
        ])


###############################################################
#                  Test get_data_description                  #
###############################################################

class TestGetDataDescription(unittest.TestCase):

    @patch('collect_tasks.prodcom_collect.os.remove')  # Mock os.remove
    @patch('collect_tasks.prodcom_collect.pd.read_csv')  # Mock pd.read_csv
    @patch('collect_tasks.prodcom_collect.get_datasets_info')  # Mock get_datasets_info
    def test_get_data_description_success(self, mock_get_datasets_info, mock_read_csv, mock_os_remove):
        dataset = 'test_dataset'

        # Mock the DataFrame returned by pd.read_csv
        mock_df = pd.DataFrame({
            'id': ['test_dataset', 'other_dataset'],
            'name': ['Test Dataset Description', 'Other Dataset Description']
        })
        mock_read_csv.return_value = mock_df

        # Call the function
        description = get_data_description(dataset)

        # Assertions
        self.assertEqual(description.strip(), 'Test Dataset Description')  # The returned description should match
        mock_get_datasets_info.assert_called_once_with('dataflow', '.')  # Ensure get_datasets_info was called with the correct arguments
        mock_read_csv.assert_called_once_with('dataflow.csv')  # Ensure read_csv was called with the correct file name
        mock_os_remove.assert_has_calls([
            call('dataflow.csv'),
            call('dataflow.xml')
        ], any_order=False)


###############################################################
#                       Test get_data                         #
###############################################################

class TestGetData(unittest.TestCase):

    @patch('collect_tasks.prodcom_collect.save_request')  # Mock the save_request function
    def test_get_data_success(self, mock_save_request):
        dataset = 'test_dataset'
        save_dir = '/path/to/save'

        # Call the function
        get_data(dataset, save_dir)

        # Assertions
        mock_save_request.assert_called_once_with(
            url=f"https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/{dataset}/?format=SDMX-CSV&compressed=true&i",
            params={},
            path=save_dir,
            file_stem=dataset,
            create_path=True,
            overwrite=True,
            want_zip=True
        )


###############################################################
#                   Test prodcom_collect                      #
###############################################################

class TestProdcomCollect(unittest.TestCase):

    @patch('collect_tasks.prodcom_collect.get_data_version')  # Mock get_data_version
    @patch('collect_tasks.prodcom_collect.get_data_columns')  # Mock get_data_columns
    @patch('collect_tasks.prodcom_collect.get_data_description')  # Mock get_data_description
    @patch('collect_tasks.prodcom_collect.get_data')  # Mock get_data
    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    def test_prodcom_collect_no_previous_version(self, mock_get_metadata, mock_get_data, mock_get_data_description, mock_get_data_columns, mock_get_data_version):
        # Setup mock objects
        mock_repo = MagicMock()
        mock_repo.get_latest_version.side_effect = Exception("No previous version")  # Simulate no previous version
        mock_config = MagicMock()
        mock_config.resource_repository = mock_repo
        mock_config.schemas.DataResource = MagicMock()

        mock_get_data_version.return_value = '20230810'
        mock_get_data_columns.return_value = ['column1', 'column2']
        mock_get_data_description.return_value = 'Test Dataset Description'

        # Call the function
        did_update = prodcom_collect(mock_config)

        # Assertions
        self.assertTrue(did_update)
        mock_get_data.assert_called()
        mock_get_metadata.assert_called()

        # Get the number of datasets used in the test
        datasets = ['dataset1', 'dataset2']

        # Ensure add_or_update_resource_list is called once for each dataset
        self.assertEqual(mock_repo.add_or_update_resource_list.call_count, len(datasets))

    @patch('collect_tasks.prodcom_collect.get_data_version')  # Mock get_data_version
    @patch('collect_tasks.prodcom_collect.get_data_columns')  # Mock get_data_columns
    @patch('collect_tasks.prodcom_collect.get_data_description')  # Mock get_data_description
    @patch('collect_tasks.prodcom_collect.get_data')  # Mock get_data
    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    def test_prodcom_collect_existing_version_no_update(self, mock_get_metadata, mock_get_data, mock_get_data_description, mock_get_data_columns, mock_get_data_version):
        # Setup mock objects
        mock_repo = MagicMock()
        mock_repo.get_latest_version.return_value = '20230810'  # Latest version matches online version
        mock_config = MagicMock()
        mock_config.resource_repository = mock_repo
        mock_config.schemas.DataResource = MagicMock()

        mock_get_data_version.return_value = '20230810'  # Same as the latest version
        mock_get_data_columns.return_value = ['column1', 'column2']
        mock_get_data_description.return_value = 'Test Dataset Description'

        # Call the function
        did_update = prodcom_collect(mock_config)

        # Assertions
        self.assertFalse(did_update)
        mock_repo.add_or_update_resource_list.assert_not_called()
        mock_get_data.assert_not_called()
        mock_get_metadata.assert_not_called()

    @patch('collect_tasks.prodcom_collect.get_data_version')  # Mock get_data_version
    @patch('collect_tasks.prodcom_collect.get_data_columns')  # Mock get_data_columns
    @patch('collect_tasks.prodcom_collect.get_data_description')  # Mock get_data_description
    @patch('collect_tasks.prodcom_collect.get_data')  # Mock get_data
    @patch('collect_tasks.prodcom_collect.get_metadata')  # Mock get_metadata
    def test_prodcom_collect_existing_version_update(self, mock_get_metadata, mock_get_data, mock_get_data_description, mock_get_data_columns, mock_get_data_version):
        # Setup mock objects
        mock_repo = MagicMock()
        mock_repo.get_latest_version.return_value = '20230809'  # Simulate an older version exists
        mock_config = MagicMock()
        mock_config.resource_repository = mock_repo
        mock_config.schemas.DataResource = MagicMock()

        mock_get_data_version.return_value = '20230810'  # Newer version is available online
        mock_get_data_columns.return_value = ['column1', 'column2']
        mock_get_data_description.return_value = 'Test Dataset Description'

        # Call the function
        did_update = prodcom_collect(mock_config)

        # Assertions
        self.assertTrue(did_update)
        mock_get_data.assert_called()
        mock_get_metadata.assert_called()
        
        # Get the number of datasets used in the test
        datasets = ['dataset1', 'dataset2']

        # Ensure add_or_update_resource_list is called once for each dataset
        self.assertEqual(mock_repo.add_or_update_resource_list.call_count, len(datasets))


if __name__ == '__main__':
    unittest.main()
