import os
from google.cloud import storage
import yaml

class Loader:
    """
    A class to handle the loading of extracted data to Google Cloud Storage.

    Attributes:
    ----------
    config : dict
        Configuration loaded from the YAML file.
    client : storage.Client
        Google Cloud Storage client.
    bucket_name : str
        Name of the Google Cloud Storage bucket.
    
    Methods:
    -------
    __init__(self, config_path):
        Initializes the Loader with configurations from a YAML file.
    upload_to_gcp(self, file_path, destination_path):
        Uploads a file to the specified Google Cloud Storage bucket at the specified destination path.
    """

    def __init__(self, config_path):
        """
        Initializes the Loader with configurations from a YAML file.

        Parameters:
        ----------
        config_path : str
            Path to the YAML configuration file.
        """
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.client = storage.Client()
        self.bucket_name = self.config['gcp_bucket']

    def upload_to_gcp(self, file_path, destination_path):
        """
        Uploads a file to the specified Google Cloud Storage bucket at the specified destination path.

        Parameters:
        ----------
        file_path : str
            Path to the file to be uploaded.
        destination_path : str
            Destination path within the bucket.
        """
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_filename(file_path)
        print(f"File {file_path} uploaded to bucket {self.bucket_name} at {destination_path}.")


if __name__ == "__main__":
    loader = Loader(config_path='../config/config.yaml')
    raw_data_dir = '../data/raw/'
    
    for file_name in os.listdir(raw_data_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(raw_data_dir, file_name)
            loader.upload_to_gcp(file_path)
