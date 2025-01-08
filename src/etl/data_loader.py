import pandas as pd
import numpy as np
import os
import math

class DataLoader:
    def __init__(self, config):
        self.old_data_path = config['old_data_path']
        self.supplier_file = config['supplier_file']
        self.model_file = config['model_file']
        self.data_dirs = config['data_dirs']
        self.month_offset_testrun = config['month_offset_testrun']
        self.column_translations = {'海关编码': 'HS_Code', 
                        '详细产品名称': 'Detailed_Description',
                        '日期': 'Date',
                        '印度进口商': 'Indian_Importer',
                        '数量单位': 'Quantity_Units',
                        '数量': 'Quantity',
                        '美元总金额': 'Total_Dollar_Amount',
                        '美元单价': 'USD_Unit_Price',
                        '卢比总金额': 'Total_Rupees_Amount',
                        '卢比单价': 'Rupees_Unit_Price',
                        '成交外币金额': 'Trans_Amount_Foreign_Currency',
                        '成交外币单价': 'Trans_Unit_Price_Foreign_Currency',
                        '币种': 'Currency',
                        '月度': 'Monthly',
                        '国外出口商': 'Foreign_Exporter',
                        '产销洲': 'Export_Continent',
                        '印度目的港': 'Entry_Port',
                        '国外装货港': 'Shipping_Port',
                        '产品描述': 'Product_Description',
                        '卢比总税费': 'Rupees_Total_Taxes',
                        '关单号': 'Customer_Order_Number',
                        '印度港口代码': 'Indian_Port_Code',
                        '运输方式': 'Transport_Method',
                        '报关行': 'Transport_Company',
                        '报关行代码': 'Transport_Company_Code',
                        '进口商地址': 'Importer_Address',
                        '进口商邮编': 'Importer_Zip_Code',
                        '进口商企业编码': 'Importer_Company_Code',
                        '进口商城市': 'Importer_City',
                        '出口商地址': 'Exporter_Address',
                        '合同号': 'Contract_No',
                        '进出口': 'Import_Or_Export'}
        
        self.use_cols = [
            "海关编码", '详细产品名称', '日期', '印度进口商', '数量单位', '数量',
            '美元总金额', '美元单价', '卢比总金额', '卢比单价', '成交外币金额',
            '成交外币单价', '币种', '国外出口商', '产品描述', '进口商地址'
        ]


    #Load suplliers with their aliases
    def _load_suppliers(self):
        suppliers = pd.read_excel(self.supplier_file, header=2).to_dict('list')
        return {key: [v for v in value if not self._is_nan(v)] for key, value in suppliers.items()}

    @staticmethod
    def _is_nan(value):
        try:
            return math.isnan(value)
        except:
            return False


    #Load model namings per supplier
    def _load_models(self):
        models = pd.read_excel(self.model_file)
        models["Model Details"] = models["Model Details"].fillna('').astype(str)
        models['Model Family'] = models['Model Family'].astype(str)
        return models


    #Load raw trade data
    def _load_raw_data(self):
        base_dir = os.path.dirname(self.model_file)
        #Loop through each subdirectory to get the full path to each excel file
        filenames = [
            os.path.join(base_dir, subdir, file)
            for subdir in self.data_dirs
            if os.path.exists(os.path.join(base_dir, subdir))
            for file in os.listdir(os.path.join(base_dir, subdir))
        ]

        #Read each excel file into a pandas dataframe and concatenate them into a single dataframe
        dfs = [
            pd.read_excel(file, header=7, dtype={
                "数量": np.int64, '美元总金额': np.int64, '美元单价': np.int64,
                '卢比总金额': np.int64, '卢比单价': np.int64
            }, usecols=self.use_cols)
            for file in filenames
        ]
        raw_data = pd.concat(dfs)

        #Translate column descriptions
        raw_data.rename(columns=self.column_translations, inplace=True)
        raw_data = raw_data.assign(Origin_Country='')

        #Convert na values of detailed description into string
        raw_data['Detailed_Description'] = raw_data['Detailed_Description'].fillna('').astype(str)

        #Convert date strings into datetime objects and sort by date
        raw_data['Date'] = pd.to_datetime(raw_data['Date'], format="%Y/%m/%d")
        raw_data.sort_values("Date", inplace=True)

        #Filter out suppliers which are car companies
        raw_data['Foreign_Exporter'] = raw_data['Foreign_Exporter'].astype(str)
        raw_data = raw_data[~raw_data['Foreign_Exporter'].str.contains('MERCEDES|DAIMLER|VOLVO|TOYOTA|FORD|HYUNDAI|JAGUAR', case=False, regex=True)]

        return raw_data

    def load_data(self):
        print("Starting Data Loading (This may take a while)")
        raw_data = self._load_raw_data()
        models = self._load_models()
        
        old_data = pd.DataFrame()

        #Load old data from previous runs if it exists
        if os.path.exists(self.old_data_path):
            old_data = pd.read_csv(self.old_data_path)
            old_data["Date"] = pd.to_datetime(old_data["Date"], errors='coerce')
            old_data.dropna(subset=["Date"], inplace=True)

        old_latest_date = old_data["Date"].max() if not old_data.empty else None
        new_data = raw_data if old_latest_date is None else raw_data[raw_data["Date"] > old_latest_date]

        #If script is run again but no new data is available, the last month will become the new data (For testing and demo purposes)
        if new_data.empty:
            new_data = raw_data[raw_data['Date'] > raw_data['Date'].max() - pd.DateOffset(months=self.month_offset_testrun)]
            old_data = old_data[old_data['Date'] < old_data['Date'].max() - pd.DateOffset(months=self.month_offset_testrun)]

        return new_data, models, old_data