import pandas as pd
from load import Loader

class Transformer:
    """
    A class to handle the transformation of data.
    """

    def __init__(self, config_path):
        """
        Initializes the Transformer with configurations from a YAML file.

        Parameters:
        ----------
        config_path : str
            Path to the YAML configuration file.
        """
        self.loader = Loader(config_path)

    def transform_data(self, source_path):
        """
        Reads data from a CSV file, removes blank lines, and uploads the transformed data to Google Cloud Storage.

        Parameters:
        ----------
        source_path : str
            Path to the CSV file to be read from the bucket.
        """
        # Download file from GCP bucket
        temp_file_path = '/tmp/temp.csv'
        self.loader.download_from_gcp(source_path, temp_file_path)
        
        # Read data with the specified encoding
        try:
            data = pd.read_csv(temp_file_path, encoding='utf-8', on_bad_lines='skip')
        except UnicodeDecodeError:
            data = pd.read_csv(temp_file_path, encoding='latin1', on_bad_lines='skip')
        except pd.errors.ParserError as e:
            print(f"ParserError: {e}")
            return None
        
        # Remove blank lines
        data.dropna(inplace=True)
        
        # Save the transformed data to a temporary file
        transformed_file_path = '/tmp/transformed.csv'
        data.to_csv(transformed_file_path, index=False)
        
        return transformed_file_path

if __name__ == "__main__":
    transformer = Transformer(config_path='../config/config.yaml')
    source_file_path = 'raw/sample.csv'
    destination_file_path = 'integrated/sample_transformed.csv'
    
    transformer.transform_data(source_file_path, destination_file_path)
