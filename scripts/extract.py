import yaml
import os
import tempfile
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from load import Loader
from transform import Transformer

class Extractor:
    """
    A class to handle the extraction of real estate auction data from Caixa's website.
    """

    def __init__(self, config_path):
        """
        Initializes the Extractor with configurations from a YAML file.

        Parameters:
        ----------
        config_path : str
            Path to the YAML configuration file.
        """
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.download_dir = tempfile.mkdtemp()  # Use temporary directory
        self.loader = Loader(config_path=config_path)
        self.transformer = Transformer(config_path=config_path)
        self.configure_driver()

    def configure_driver(self):
        """
        Configures the Chrome WebDriver options.
        """
        self.chrome_options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        self.chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(options=self.chrome_options)

    def download_data(self, state_code):
        """
        Downloads data for a specific state and uploads it to GCS.

        Parameters:
        ----------
        state_code : str
            The code of the state to download data for (e.g., "MG" for Minas Gerais).
        """
        url = "https://venda-imoveis.caixa.gov.br/sistema/download-lista.asp"
        self.driver.get(url)

        try:
            # Wait until the <select> element is present
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "cmb_estado")))

            # Find the <select> element by ID
            select_element = self.driver.find_element(By.ID, "cmb_estado")
            
            # Create a Select object to interact with the <select> element
            select = Select(select_element)
            
            # Select the specific state
            select.select_by_value(state_code)

            # Wait until the "Next" button is present
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "btn_next1")))
            
            # Find the "Next" button by ID and click it
            next_button = self.driver.find_element(By.ID, "btn_next1")
            next_button.click()
            
            print(f"State {state_code} selected and 'Next' button clicked successfully!")
            
            # Wait for some time to ensure the download is complete
            WebDriverWait(self.driver, 30).until(
                lambda driver: any([filename.endswith('.csv') for filename in os.listdir(self.download_dir)])
            )

            # Upload the downloaded file to GCS
            for file_name in os.listdir(self.download_dir):
                if file_name.endswith('.csv'):
                    file_path = os.path.join(self.download_dir, file_name)
                    
                    # Add extraction date to the file name
                    extraction_date = datetime.now().strftime('%d_%m_%Y')
                    new_file_name = f"{os.path.splitext(file_name)[0]}_{extraction_date}.csv"
                    raw_destination_path = f"data/raw/{new_file_name}"

                    # Upload raw file to GCS
                    self.loader.upload_to_gcp(file_path, raw_destination_path)

                    # Process the file using Transformer and upload the transformed file to GCS
                    transformed_file_path = self.transformer.transform_data(raw_destination_path)
                    integrated_destination_path = f"data/integrated/{file_name}"
                    self.loader.upload_to_gcp(transformed_file_path, integrated_destination_path)
            
            print("File downloaded, uploaded, and processed successfully!")
        
        finally:
            self.driver.quit()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', 'config', 'config.yaml')
    extractor = Extractor(config_path=config_path)
    for state in extractor.config['estados']:
        extractor.download_data(state)
