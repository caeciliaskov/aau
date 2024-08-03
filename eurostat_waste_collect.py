from logging import getLogger
import xml.etree.ElementTree as ET
import pandas as pd
from dataio.config import Config
from ._utilities import *
from time import time
import os

# Global variables
logger = getLogger("root")
base_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/"
datasets = ['ENPS_ENV_WASGENH', 'ENPS_ENV_WASGENM', 'ENPS_ENV_WASTRT', 'ENPS_ENV_WAT_ABS', 'ENPS_ENV_WAT_CAT', 'ENV_WASBAT', 'ENV_WASELEE', 'ENV_WASELEEOS', 
            'ENV_WASELV', 'ENV_WASELVT', 'ENV_WASFLOW', 'ENV_WASFW', 'ENV_WASGEN', 'ENV_WASMUN', 'ENV_WASOPER', 'ENV_WASPAC', 'ENV_WASPACR', 'ENV_WASPB', 
            'ENV_WASPCB', 'ENV_WASSHIP', 'ENV_WASTRDMP', 'ENV_WASTRT', 'ENV_WW_SPD']
metadata_types = ['dataflow', 'codelist', 'conceptscheme']

# Get dataset information
def get_datasets_info(metadata_type: str, save_dir: str):
    '''
        This function retrieves and saves information on the datasets in CSV format.

        Parameters
        ----------
        metadata_type : str
            The type of metadata wanting to retrieve. See metadata_types list at the top of the script.
        save_dir : str
            The directory you want to save the datasets information in.
        
        Returns
        ----------
        None
    '''
    params = {"lang": "en", "detail": "allstubs", "completestub": "true"}
    if metadata_type in ['dataflow', 'codelist']:
        # Remove the 'lang' parameter for these endpoints
        params["lang"] = None
    else:
        params["lang"] = "en"

    logger.info(f"Get {metadata_type}")
    t1 = time()
    save_request(
        url=f"{base_url}{metadata_type}/ESTAT/all",
        params=params,
        path=save_dir,
        file_stem=metadata_type,
        create_path=True,
        overwrite=True,
        want_zip=False
    )
    logger.info(f"Got {metadata_type} in {time() - t1:.2f}s")
    xml2csv_metadata(save_dir, save_dir, metadata_type)
    os.remove(f'{save_dir}/{metadata_type}.xml')


# Download metadata
def get_metadata(id: str, metadata_type: str, save_dir: str):
    '''
        This function retrieves and saves metadata for a given dataset.

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
        None.
    '''
    logger.info(f"Starting the process to request {metadata_type} for dataset: {id}")

    try:
        # Read the CSV into a DataFrame
        get_datasets_info(metadata_type, '')
        df = pd.read_csv(f'{metadata_type}.csv')
        os.remove(f'{metadata_type}.csv')

        # Filter the row corresponding to the id
        row = df[df["id"] == id]

        # If no matching row is found, log a message and exit
        if len(row) == 0:
            logger.warning(f"No {metadata_type} found for id {id} in the CSV.")
            return

        # Extract the structure URL from the row
        structure_url = row["structureURL"].values[0]

        # Use the save_request utility function
        save_request(
            url=structure_url,
            params={},
            path=save_dir,
            file_stem=f"dim_{id}",
            create_path=True,
            overwrite=True, 
            want_zip=True)

        logger.info(f"Successfully saved {metadata_type} for {id} to {save_dir}")

    except Exception as e:
        logger.error(f"An error occurred during the {metadata_type} request process: {e}")

# Get data version
def get_data_version(dataset: str):
    '''
        This function retrieves the data version for a given dataset.

        Parameters
        ----------
        dataset : str
            The dataset wanting to retrieve the data version from.

        Returns 
        ----------
        Data version : str
    '''
    get_metadata(dataset, 'conceptscheme', '')
    unzip_files('', force_overwrite=False, destination_directory='')
    root = ET.parse(f'dim_{dataset}.xml').getroot()
    version = (root[0][2].text).split('T')[0].replace('-','')
    os.remove(f'dim_{dataset}.xml')
    os.remove(f'dim_{dataset}.zip')
    return version


# Get list of codes in the dataset
def get_data_columns(dataset: str):
    '''
        This function retrieves a list of the columns in a given dataset.

        Parameters
        ----------
        dataset : str
            The dataset wanting to retrieve the columns from.

        Returns 
        ----------
        Data columns : list
    '''
    get_metadata(dataset, 'conceptscheme', '')
    unzip_files('', force_overwrite=False, destination_directory='')
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

# Download data
def get_data(dataset: str, save_dir: str):
    '''
        This function retrieves and saves data for a given dataset in zipped CSV format.

        Parameters
        ----------
        dataset : str
            The dataset wanting to retrieve data from.
        save_dir : str
            The directory you want to save the datasets information in.

        Returns 
        ----------
        None.
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
    repo = config.resource_repository
    did_update = False

    for dataset in datasets:
        resource = config.schemas.DataResource(
            name = f'eurostat_waste_{dataset}', 
            schema_name = "None", 
            task_name = "eurostat_waste_collect",
            stage = "collect",
            location = f"collect/eurostat/{dataset}/{resource.data_version}",
            comment = f"Data and metadata collected from eurostat for dataset {dataset}",
            created_by = "Albert K. Osei-Owusu",
            license = "Open Data Commons Public Domain Dedication (CC-BY 4.0)",
            license_url = "https://creativecommons.org/licenses/by-sa/4.0/legalcode"),

        online_version = get_data_version(dataset)
        code_list = get_data_columns(dataset)

        try:
            latest_version = repo.get_latest_version(name=resource.name, stage=resource.stage, task_name=resource.task_name)
        except:
            latest_version = None
        if latest_version:
            if latest_version != online_version:
                resource.data_version = online_version
                get_data(dataset, resource.location)
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
            for metadata in metadata_types:
                    if metadata == 'codelist':
                        for id in code_list:
                            get_metadata(id, metadata, f'{resource.location}/metadata')
                    else:
                        get_metadata(dataset, metadata, f'{resource.location}/metadata')
            repo.add_or_update_resource_list(resource)
            did_update = True
    return did_update
