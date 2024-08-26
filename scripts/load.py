import os
import io 
from google.cloud import storage
import yaml
import pandas as pd

class Loader:
    """
    A class to handle the loading of extracted data to Google Cloud Storage.
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

    def download_from_gcp(self, source_path, destination_path):
        """
        Downloads a file from the specified Google Cloud Storage bucket to the local destination path.

        Parameters:
        ----------
        source_path : str
            Path within the bucket to the file to be downloaded.
        destination_path : str
            Local path where the file should be downloaded.
        """
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(source_path)
        blob.download_to_filename(destination_path)
        print(f"File {source_path} downloaded from bucket {self.bucket_name} to {destination_path}.")

    def read_data(self, file_path):
        """
        Reads data from a CSV file into a Pandas DataFrame.

        Parameters:
        ----------
        file_path : str
            Path to the CSV file to be read.
        
        Returns:
        -------
        pd.DataFrame
            DataFrame containing the data from the CSV file.
        """
        return pd.read_csv(file_path)
    
    def read_csv_from_gcp(self, source_path):
        """
        Reads a CSV file directly from the specified Google Cloud Storage bucket and returns a DataFrame.

        Parameters:
        ----------
        source_path : str
            Path within the bucket to the file to be read.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the data from the CSV file.
        """
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(source_path)
        data = blob.download_as_text()
        
        # Leia os dados como um DataFrame sem cabeçalhos e pule a primeira linha que contém o cabeçalho atual
        df = pd.read_csv(io.StringIO(data), sep=',', header=None, skiprows=1)
        
        # Defina a primeira linha lida (agora no índice 0) como cabeçalho do DataFrame
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])  # Remove a linha que agora serve como cabeçalho
        
        # Resetar o índice após as modificações
        df.reset_index(drop=True, inplace=True)
        
        return df


if __name__ == "__main__":
    loader = Loader(config_path='../config/config.yaml')
    raw_data_dir = '../data/raw/'
    
    for file_name in os.listdir(raw_data_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(raw_data_dir, file_name)
            loader.upload_to_gcp(file_path)

