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
        
        # Tenta ler os dados, definindo a primeira linha como cabeçalhos e tratando possíveis erros
        try:
            # Leia o arquivo sem considerar a primeira linha como cabeçalho
            data = pd.read_csv(temp_file_path, encoding='utf-8', sep=';', header=None, skiprows=1, on_bad_lines='warn')
        except UnicodeDecodeError:
            data = pd.read_csv(temp_file_path, encoding='latin1', sep=';', header=None, skiprows=1, on_bad_lines='warn')
        except pd.errors.ParserError as e:
            print(f"ParserError: {e}")
            return None
        
        # Definindo a primeira linha de dados como cabeçalho
        data.columns = data.iloc[0]
        data = data[1:]

        # Resetar o índice do DataFrame após redefinir os cabeçalhos
        data.reset_index(drop=True, inplace=True)
        
        # Save the transformed data to a temporary file
        transformed_file_path = '/tmp/transformed.csv'
        data.to_csv(transformed_file_path, index=False)
        
        return transformed_file_path

if __name__ == "__main__":
    transformer = Transformer(config_path='../config/config.yaml')
    source_file_path = 'raw/sample.csv'
    destination_file_path = 'integrated/sample_transformed.csv'
    
    transformer.transform_data(source_file_path, destination_file_path)
