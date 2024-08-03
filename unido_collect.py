# Import packages
import json
from logging import getLogger
from pathlib import Path

import requests
from dataio.config import Config
from templates import set_logger

logger = getLogger('root')
set_logger(
    filename=("unido_collect.log"),
    path="",
    log_level=20,
    create_path=True,
)


# Optional step: Get metadata information for a dataset
def get_metadata(dataset: str):

  """
    This function fetches and saves metadata information in json format.

    Parameters
    ------------
    dataset : str
      Name of the dataset wanting to retrive.

    Returns
    ------------
    Json response data for the dataset.

  """

  logger.info(f"Starting the process to request metadata for dataset: {dataset}")

  # Make a request for the dataset
  dataset_url = 'https://stat.unido.org/portal/dataset/getDataset/{}'.format(dataset)
  response = requests.get(dataset_url)
  data = json.loads(response.text)
  
  logger.info(f"Successfully retrieved metadata for {dataset}")

  return data


# Optional step: Get data values for specific parameters
def get_data_values(dataset: str, activities: list, countries: list, variables: list, periods: list):
  
  """
    This function fetches and saves data values as a json file.
   
    Parameters
    ----------
    dataset : str
      Name of the dataset wanting to retrive.
    activities : list
      List of the activities to include.
    countries : list
      List of the countries to include.
    variable : list
      List of the variables to include.
    periods : list
      List of the years to include.

    Returns
    ----------
    List of dict with data values.

  """

  meta_data = get_metadata(dataset)
  dataset_id = meta_data['id']

  logger.info(f"Starting the process to request data")

  data_url = 'https://stat.unido.org/portal/dataset/getData'
  all_data = []
  for cc in countries:
    logger.info(f"Starting process for country: {cc}")
    for vc in variables:
      logger.info(f"Starting process for variable: {vc}")
      data = {
      "datasetId": dataset_id,
      "countryCode": cc,
      "variableCode": vc,
      "activityCodes": activities,
      "periods": periods
      }
      response = requests.post(data_url, json=data)
      json_result = json.loads(response.text)['data']
      for i in json_result:
          i.update({'cc': cc, 'vc': vc})
          all_data.append(i)
      logger.info(f"Finished process for variable: {vc}")  
    logger.info(f"Finished process for country: {cc}")
  
  logger.info(f"Successfully retrieved data for {dataset}")

  return all_data


# Get data version
def get_data_version(dataset:str):
  """
    This function extracts the production year of the data.

    Parameters
    ----------
    Dataset : str
      Name of the dataset.

    Returns
    -------
    String with production year.

  """

  data = get_metadata(dataset)
  return data['production_year']


# Download the complete dataset
def download_unido_data(dataset:str, save_directory:str):
  
  """
    This function downloads a specific dataset completely and saves as a json file.
   
    Parameters
    ----------
    dataset : str
      Name of the dataset wanting to retrive.
    save_directory : str
      Directory where the response content should be saved. 
      If left empty, response will be saved in the current working directory.

    Returns
    ----------
    None.

  """
  directory_path = Path(save_directory)
  data = get_metadata(dataset)
  countries = []
  periods = data['periods']
  activities = []
  variables = []
  for i in data['countries']:
      countries.append(i['c'])
  for i in data['activities']:
      activities.append(i['c'])
  for i in data['variables']:
      variables.append(i['c'])
  result = get_data_values(dataset, activities, countries, variables, periods)

  with open('{}/fact_{}.json'.format(directory_path, dataset), 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=4)

  logger.info(f"Dataset {dataset} successfully downloaded and saved to {save_directory}")


def unido_collect(config: Config) -> bool:
   
  required_resources = [
    config.schemas.DataResource(
      name = "INDSTAT", 
      schema_name = "None", 
      task_name = "unido",
      stage = "collect",
      location = "collect/unido/INDSTAT/{version}",
      comment = "Data collected from UNIDO for INDSTAT",
      created_by = "Cæcilia Lind Skov-Jensen",
      license = "Creative Commons Attribution 4.0 International License",
      license_url = "https://stat.unido.org/terms-and-conditions"),

    config.schemas.DataResource(
      name = "IDSB", 
      schema_name = "None", 
      task_name = "unido",
      stage = "collect",
      location = "collect/unido/IDSB/{version}",
      comment = "Data collected from UNIDO for IDSB",
      created_by = "Cæcilia Lind Skov-Jensen",
      license = "Creative Commons Attribution 4.0 International License",
      license_url = "https://stat.unido.org/terms-and-conditions")
    ]

  repo = config.resource_repository

  did_update = False

  for resource in required_resources:
    online_version = get_data_version(resource.name)
    try:
      latest_version = repo.get_latest_version(name=resource.name, stage=resource.stage, task_name=resource.task_name)
    except:
      latest_version = None
    
    if latest_version:
      if latest_version != online_version:
        resource.data_version = online_version
        download_unido_data(resource.name, resource.location)
        repo.add_or_update_resource_list(resource)
        did_update = True
    else:
      if online_version:
        resource.data_version = online_version
      else:
        resource.data_version = "YYYY"
      download_unido_data(resource.name, resource.location)
      repo.add_or_update_resource_list(resource)
      did_update = True

  return did_update
   