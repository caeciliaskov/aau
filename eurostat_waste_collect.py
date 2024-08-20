import logging
import xml.etree.ElementTree as ET
import pandas as pd
from dataio.config import Config
from ._utilities import save_request, xml2csv_metadata, unzip_files, getLogger
from time import time
import os
from zipfile import ZipFile, ZIP_DEFLATED
import json
import io
from xml.dom.minidom import parseString
import requests
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("eurostat_waste.log"),
                        logging.StreamHandler()
                    ])

# Global variables
logger = getLogger("root")
base_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/"
datasets = ['ENPS_ENV_WASGENH', 'ENPS_ENV_WASGENM', 'ENPS_ENV_WASTRT', 'ENPS_ENV_WAT_ABS', 'ENPS_ENV_WAT_CAT', 'ENV_WASBAT', 'ENV_WASELEE', 'ENV_WASELEEOS', 
            'ENV_WASELV', 'ENV_WASELVT', 'ENV_WASFLOW', 'ENV_WASFW', 'ENV_WASGEN', 'ENV_WASMUN', 'ENV_WASOPER', 'ENV_WASPAC', 'ENV_WASPACR', 'ENV_WASPB', 
            'ENV_WASPCB', 'ENV_WASSHIP', 'ENV_WASTRDMP', 'ENV_WASTRT', 'ENV_WW_SPD']
metadata_types = ['dataflow', 'codelist', 'conceptscheme']


# Prettify xml data
def prettify_data(data: str, file_extension: str) -> str:
    """
        Prettify data based on its file extension.
        
        Parameters
        -----------
        data : str
            The data to be prettified.
        file_extension : str
            The type of data.
        
        Returns
        -----------
        The data in a prettified form.

    """
    if file_extension == ".json":
        return json.dumps(json.loads(data), indent=4)
    elif file_extension == ".xml":
        try:
            dom = parseString(data)
            return dom.toprettyxml(indent="  ")
        except Exception as e:
            logger.error(f"Failed to prettify XML: {e}")
            return data
    elif file_extension == ".csv":
        df = pd.read_csv(io.StringIO(data))
        return df.to_csv(index=False)
    elif file_extension == ".tsv":
        df = pd.read_csv(io.StringIO(data), delimiter="\t")
        return df.to_csv(index=False, sep="\t")
    else:
        return data


# Save prettified xml to zip
def save_prettified_xml_to_zip(xml_data: str, output_zip_path: str, xml_filename: str) -> None:
    """
        Save prettified XML data to a zip file.

        Parameters
        -------------
        xml_data: str 
            The raw XML data as a string.
        output_zip_path: str 
            The path to the output zip file.
        xml_filename: str 
            The name of the XML file inside the zip archive.

        Returns
        ------------
        None
    """
    try:
        # Prettify the XML data
        prettified_xml = prettify_data(xml_data, ".xml")
        
        # Create a zip file and add the prettified XML
        with ZipFile(output_zip_path, 'w', ZIP_DEFLATED) as zip_file:
            zip_file.writestr(xml_filename, prettified_xml)
        
        logger.info(f"Prettified XML saved to {output_zip_path} as {xml_filename}")
    except Exception as e:
        logger.error(f"Failed to save prettified XML to zip: {e}")

        
# Get dataset information
def get_datasets_info(metadata_type: str, save_dir: str):
    params = {"lang": "en", "detail": "allstubs", "completestub": "true"}
    if metadata_type in ['dataflow', 'codelist']:
        params["lang"] = None
    else:
        params["lang"] = "en"

    logger.info(f"Get {metadata_type}")
    t1 = time()
    url = f"{base_url}{metadata_type}/ESTAT/all"
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # This will raise an HTTPError for bad responses
        data = response.content
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from {url}: {e}")
        logger.info(f"Got {metadata_type} in {time() - t1:.2f}s")
        raise  # Ensure that we return here so that file saving and xml2csv_metadata are not called

    prettified_data = prettify_data(data, '.xml')

    # Save prettified data to file
    try:
        # Ensure the directory exists before saving the file
        os.makedirs(save_dir, exist_ok=True)
        with open(f'{save_dir}/{metadata_type}.xml', 'w', encoding='utf-8') as file:
            file.write(prettified_data)
        logger.info(f"Data has been prettified and saved to {save_dir}.")
    except Exception as e:
        logger.error(f"Failed to save data to file {save_dir}: {e}")
        logger.info(f"Got {metadata_type} in {time() - t1:.2f}s")
        return  # Ensure that we return here so that xml2csv_metadata is not called on failure

    xml2csv_metadata(save_dir, save_dir, metadata_type)

# Download metadata
def get_metadata(id: str, metadata_type: str, save_dir: str):
    '''
        Retrieve and save metadata for a given dataset.

        Parameters
        ----------
        id : str
            ID of the dataset for which the metadata is to be fetched.
            For "conceptscheme", it refers to the dataset ID.
            For "codelist", it refers to the dimension ID.
        metadata_type : str
            Type of metadata being fetched, which will be used for filename prefixing.
            Examples include "codelist" or "conceptscheme".
        save_dir : str
            Directory where the response content should be saved.
        
        Returns
        ----------
        None
    '''
    logger.info(f"Starting the process to request {metadata_type} for dataset: {id}")

    try:
        os.makedirs(save_dir, exist_ok=True)
        # Read the CSV into a DataFrame
        get_datasets_info(metadata_type, '.')
        df = pd.read_csv(f'{metadata_type}.csv')
        os.remove(f'{metadata_type}.csv')
        os.remove(f'{metadata_type}.xml')

        # Filter the row corresponding to the id
        row = df[df["id"] == id]

        # If no matching row is found, log a message and exit
        if len(row) == 0:
            logger.warning(f"No {metadata_type} found for id {id} in the CSV.")
            return

        # Extract the structure URL from the row
        url = row["structureURL"].values[0]

        response = requests.get(url, params={})        
        zip_filename = f"dim_{id}.zip"
        zip_filepath = Path(save_dir) / zip_filename
        xml_filename = f"dim_{id}.xml"
        save_prettified_xml_to_zip(response.content, zip_filepath, xml_filename)

        logger.info(f"Successfully saved {metadata_type} for {id} to {save_dir}")

    except Exception as e:
        logger.error(f"An error occurred during the {metadata_type} request process: {e}")

# Get data version
def get_data_version(dataset: str):
    '''
        Retrieve the data version for a given dataset.

        Parameters
        ----------
        dataset : str
            The dataset wanting to retrieve the data version from.

        Returns 
        ----------
        String with data version in format "YYYYMMDD".
    '''
    get_metadata(dataset, 'conceptscheme', '.')
    unzip_files('.')
    root = ET.parse(f'dim_{dataset}.xml').getroot()
    version = (root[0][2].text).split('T')[0].replace('-','')
    os.remove(f'dim_{dataset}.xml')
    os.remove(f'dim_{dataset}.zip')
    return version


# Get columns in dataset
def get_data_columns(dataset: str):
    '''
        Retrieve a list of the columns in a given dataset.

        Parameters
        ----------
        dataset : str
            The dataset wanting to retrieve the columns from.

        Returns 
        ----------
        List of data columns.
    '''
    get_metadata(dataset, 'conceptscheme', '.')
    unzip_files('.')
    root = ET.parse(f'dim_{dataset}.xml').getroot()
    NS = {"m": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
          "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
          "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
          "xml": "http://www.w3.org/XML/1998/namespace"}
    ids = []
    for item in root.findall(f".//s:Enumeration", NS):
        ids.append(item[0].get('id'))
    os.remove(f'dim_{dataset}.xml')
    os.remove(f'dim_{dataset}.zip')
    return ids

# Get dataset description
def get_data_description(dataset: str):
    '''
        Retrieve a description of the dataset.

        Parameters
        ------------
        dataset : str
            The name of the dataset.

        Returns
        ------------
        String with the dataset description.
    '''
    get_datasets_info('dataflow', '.')
    df = pd.read_csv('dataflow.csv')
    filtered_df = df[df['id'] == dataset]
    os.remove('dataflow.csv')
    os.remove('dataflow.xml')
    return filtered_df['name'].to_string(index=False)

# Download/update data
def get_data(dataset: str, save_dir: str):
    '''
        Retrieve and save data for a given dataset in zipped CSV format.

        Parameters
        ----------
        dataset : str
            The dataset wanting to retrieve data from.
        save_dir : str
            The directory you want to save the datasets information in.

        Returns 
        ----------
        None
    '''
    save_request(
        url=f"{base_url}data/{dataset}/?format=SDMX-CSV&compressed=true&i",
        params={},
        path=save_dir,
        file_stem=dataset,
        create_path=True,
        overwrite=True,
        want_zip = True)


def eurostat_collect(config: Config) -> bool:
    '''
        Add or update the data collected in Sharepoint as well as update the resources file.
    '''
    repo = config.resource_repository
    did_update = False

    for dataset in datasets:
        resource = config.schemas.DataResource(
            name=f'eurostat_waste_{dataset}',
            schema_name="None",
            location="", 
            task_name="eurostat_waste_collect",
            stage="collect",
            data_flow_direction='output',
            data_version="00000000",
            code_version="0.0.0",
            comment=f"Waste data and metadata collected from eurostat for dataset {dataset}",
            created_by="Albert K. Osei-Owusu",
            license="Open Data Commons Public Domain Dedication (CC-BY 4.0)",
            license_url="https://creativecommons.org/licenses/by-sa/4.0/legalcode",
            description="",
            url=""
        )

        resource.location = f"collect/eurostat/{dataset}/{resource.data_version}"

        online_version = get_data_version(dataset)
        code_list = get_data_columns(dataset)
        data_description = get_data_description(dataset)

        try:
            latest_version = repo.get_latest_version(name=resource.name, stage=resource.stage, task_name=resource.task_name)
        except:
            latest_version = None
        if latest_version:
            if latest_version != online_version:
                resource.data_version = online_version
                get_data(dataset, resource.location)
                resource.description = data_description
                for metadata in metadata_types:
                    if metadata == 'codelist':
                        for id in code_list:
                            get_metadata(id, metadata, f'{resource.location}/metadata')
                    else:
                        get_metadata(dataset, metadata, f'{resource.location}/metadata')
                repo.add_or_update_resource_list(resource)
                did_update = True
        else:
            if online_version:
                resource.data_version = online_version
            else:
                resource.data_version = "00000000"
            get_data(dataset, resource.location)
            resource.description = data_description
            for metadata in metadata_types:
                    if metadata == 'codelist':
                        for id in code_list:
                            get_metadata(id, metadata, f'{resource.location}/metadata')
                    else:
                        get_metadata(dataset, metadata, f'{resource.location}/metadata')
            repo.add_or_update_resource_list(resource)
            did_update = True
    return did_update
