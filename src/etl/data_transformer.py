from etl.transform_functions import USD_EUR_Conversion, map_competitor, map_compressor, KGS_Outlier_Handling, string_match, exclude_parts, preprocess_mapping
import pandas as pd

class DataTransformer:

    """Class to perform data transformation on the raw data."""

    def __init__(self, config):
        self.usd_eur_file = config["usd_eur_file"]
        self.usd_eur = self._load_usd_eur_conversion_data()


    def _load_usd_eur_conversion_data(self):

        """Load and preprocess the USD/EUR conversion data."""

        # Read the USD/EUR conversion data, extract the date column and sort the data by date
        usd_eur = pd.read_csv(self.usd_eur_file)
        usd_eur["DATE"] = pd.to_datetime(usd_eur["DATE"], format=r"%Y-%m-%d")
        usd_eur.sort_values("DATE", inplace=True)

        # Fill missing days in the exchange rate column using forward and backward fill, so that each day has an exchange rate
        usd_eur.set_index('DATE', inplace=True)
        full_date_range = pd.date_range(start=usd_eur.index.min(), end=usd_eur.index.max())
        usd_eur = usd_eur.reindex(full_date_range)
        usd_eur['Euro/US dollar (EXR.D.USD.EUR.SP00.A)'] = usd_eur['Euro/US dollar (EXR.D.USD.EUR.SP00.A)'].ffill().bfill()

        # Reset the index and rename 'index' to 'DATE'
        usd_eur.reset_index(inplace=True)
        usd_eur.rename(columns={'index': 'DATE'}, inplace=True)
        return usd_eur

    def transform(self, raw_data, models):

        """Perform the data transformation process."""

        raw_data = raw_data.copy()
        models = models.copy()

        print("Starting Data transformation")

        # Convert USD to EUR
        raw_data = USD_EUR_Conversion(raw_data, self.usd_eur)

        # Drop rows with non-positive quantities
        raw_data = raw_data[raw_data["Quantity"] > 0]

        # Convert strings to uppercase for consistent string matching
        for column in ["Indian_Importer", "Foreign_Exporter", "Detailed_Description"]:
            raw_data[column] = raw_data[column].str.upper()

        # Categorize by main competitors, normalize competitor names
        competitor_mapping = {
            'Frascold': ['FRASCOLD'], 
            'MYCOM': ['MAYEKAWA'],
            'Snowman': ['FUJIAN', 'SRM', 'SNOWMAN'],
            'Hanbell': ['HANBELL', 'COMER'],
            'Fu Sheng': ['FU SHENG', 'FUSHENG'],
            'Daikin': ['DAIKIN'],
            'J&E Hall': ['J&E', 'J & E'],
            'GEA': ['GEAREFRIG', 'GEA REFRIG'],
            'Dorin': ['DORIN'],
            'Bock': ['BOCK'],
            'Danfoss': ['DANFOSS'],
            'Copeland/Emerson': ['EMERSON', 'COPELAND'],
            'BITZER': ['BITZER'],
            'Siam': ['SIAM'],
            'Invotech': ['INVOTECH']
        }
        raw_data["Competitor"] = raw_data["Foreign_Exporter"].apply(map_competitor, args=(competitor_mapping,))

        # Categorize compressor types if these types are present in the Detailed_Description
        compressor_mapping = {
            'Recip': 'RECIP',
            'Scroll': 'SCROLL',
            'Screw': 'SCREW',
            'Rotary': 'ROTARY'
        }
        raw_data['Compressor_Type'] = raw_data['Detailed_Description'].apply(lambda x: map_compressor(x, compressor_mapping))

        # Add Bock model mapping
        bock_mapping = {
            '14057': ' FEX ',
            '14056': ' FKX ',
            '20250': ' FKX ',
            '20071': ' FKX ',
            '11712': ' F16 ',
            '11702': ' F16 ',
            '11700': ' F16 ',
            '144': ' HG '
        }
        for key, value in bock_mapping.items():
            raw_data.loc[raw_data['Competitor'] == 'Bock', 'Detailed_Description'] = raw_data.loc[raw_data['Competitor'] == 'Bock', 'Detailed_Description'].str.replace(key, value)

        # Exclude irrelevant deliveries like parts
        raw_data = exclude_parts(raw_data, models)

        # Handle special cases for BITZER
        raw_data.loc[raw_data['Competitor'] == 'BITZER', 'Detailed_Description'] = raw_data.loc[raw_data['Competitor'] == 'BITZER', 'Detailed_Description'].str.replace(' ', '_')

        # Perform model matching
        print("Starting Model Matching (This may take a while)")
        mapping_preprocessed = preprocess_mapping(models)
        raw_data[['models', 'comp_types', 'comp_family']] = raw_data.apply(
            lambda row: pd.Series(string_match(row["Detailed_Description"], row["Competitor"], mapping_preprocessed)), 
            axis=1
        )
        print("Model Matching complete!")

        # Fill missing values in compressor family
        raw_data["comp_family"] = raw_data["comp_family"].fillna("Unknown_Family")

        # Merge Compressor_Type into comp_types, so types inferred from the recognized model override types seen in the Detailed_Description
        raw_data['comp_types'] = raw_data['comp_types'] + raw_data['Compressor_Type']
        raw_data.drop("Compressor_Type", axis=1, inplace=True)

        raw_data['comp_types'] = raw_data['comp_types'].replace({
            'RecipRecip': 'Recip',
            'ScrewScrew': 'Screw',
            'ScrollScroll': 'Scroll',
            'ScrollScrew': 'Scroll',
            'RecipScrew': 'Recip',
            'ScrollRecip': 'Scroll',
            'ScrewRecip': 'Screw',
            'ScrewScroll': 'Screw',
            'RecipScroll': 'Recip',
            'RecipRotary': 'Recip',
            'ScrewRotary': 'Screw',
            'ScrollRotary': 'Scroll',
            'Open-typeRecip': 'Open-type',
            'Open-typeScrew': 'Open-type',
            'Open-typeScroll': 'Open-type',
            'ACPScrew': 'ACP',
            'ACPRecip': 'ACP',
            'ACPScroll': 'ACP',
            '': 'Unknown Type',
            ' ': 'Unknown Type'
        }).fillna('Unknown Type')

        # Handle outliers for KGS
        raw_data = KGS_Outlier_Handling(raw_data, self.usd_eur)

        print("Rows of new data after transformation: ", len(raw_data.index))
        print("Data Transformation complete!")
        return raw_data