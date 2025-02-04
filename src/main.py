from etl.data_loader import DataLoader
from etl.data_transformer import DataTransformer
from etl.data_exporter import DataExporter
from etl.data_tester import DataDistributionTests

class ETLOrchestrator:
    def __init__(self, config):
        self.data_loader = DataLoader(config)
        self.data_transformer = DataTransformer(config)
        self.data_exporter = DataExporter(config)
        self.data_distribution_test = DataDistributionTests()

    def run(self):
        # Step 1: Load Data
        new_data, models, old_data = self.data_loader.load_data()

        # Step 2: Transform Data
        transformed_new_data = self.data_transformer.transform(new_data, models)

        # Step 3: Test Data
        self.data_distribution_test.test_distribution(transformed_new_data, old_data)

        # Step 4: Export Data
        self.data_exporter.export(transformed_new_data, old_data)

if __name__ == "__main__":
    config = {
            "old_data_path": "C:/Tradedata_output/data.csv",
            "supplier_file": "K:/DESDN/mbd/pm/mpm_pma/00_Projekte/CSMO/Market Assessment/Market APAC/India/Handelsdatenprojekt/Daten/Supplier Names India.xlsx",
            "model_file": "K:/DESDN/mbd/pm/mpm_pma/00_Projekte/CSMO/Market Assessment/Market APAC/India/Handelsdatenprojekt/Daten/Model Mapping.xlsx",
            "data_dirs": ["2021", "2022", "2023", "2024"],
            "month_offset_testrun": 48, #This value determines how many months back the testrun should be done when no new data is available
            "export_path": "C:/Tradedata_Output/",
            "usd_eur_file": "K:/DESDN/mbd/pm/mpm_pma/00_Projekte/CSMO/Market Assessment/Market APAC/India/Handelsdatenprojekt/Daten/USD_EUR.csv"
    }

    etl = ETLOrchestrator(config)
    etl.run()