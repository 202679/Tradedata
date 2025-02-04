import pandas as pd
import os

class DataExporter:
    """
    Handles exporting transformed data to specified formats and locations.
    """
    def __init__(self, config):
        self.output_dir = config["export_path"]
        self.ensure_output_directory()

    def ensure_output_directory(self):
        """
        Ensures the output directory exists. Creates it if not present.
        """
        if not os.path.exists(str(self.output_dir)):
            try:
                os.makedirs(str(self.output_dir))
                print(f"Directory {str(self.output_dir)} created.")
            except OSError as e:
                print(f"Failed to create directory {str(self.output_dir)}")
        else:
            print(f"Directory {str(self.output_dir)} already exists.")

    def export(self, new_data: pd.DataFrame, old_data: pd.DataFrame):
        """
        Exports the combined dataset to Excel and CSV formats.

        Args:
            new_data (pd.DataFrame): Transformed new data.
            old_data (pd.DataFrame): Existing old data.
        """
        print("Starting Data Extraction")
        
        # Add Year and Month columns for filtering
        new_data['Year'] = new_data['Date'].dt.year
        new_data['Month'] = new_data['Date'].dt.month

        max_month = str(new_data['Month'].max())
        max_year = str(new_data['Year'].max())

        # Combine old and new data
        combined_data = pd.concat([old_data, new_data])

        # Drop duplicates
        combined_data.drop_duplicates(inplace=True)

        # Convert all float columns to integer
        float_columns = combined_data.select_dtypes(include=['float64']).columns
        if not float_columns.empty:
            combined_data[float_columns] = combined_data[float_columns].fillna(0)
            combined_data[float_columns] = combined_data[float_columns].astype(int)

        # Prepare Excel output with selected columns
        excel_output = combined_data[[
            "Year", "Month", "Competitor", "comp_types", "comp_family", "models", 
            "Indian_Importer", "Detailed_Description", "Total_Euro_Amount", 
            "Total_Rupees_Amount", "Quantity"
        ]]

        # Export to Excel
        excel_file_path = os.path.join(str(self.output_dir), f"Import Data India_Compressors_{max_month}_{max_year}.xlsx")
        excel_output.to_excel(excel_file_path, index=False)
        print(f"Excel file for sending exported to: {excel_file_path}")

        # Export to CSV
        csv_file_path = os.path.join(str(self.output_dir), "data.csv")
        combined_data.to_csv(csv_file_path, index=False)
        print(f"CSV file for PowerBI exported to: {csv_file_path}")

        print("Data Extraction complete!")