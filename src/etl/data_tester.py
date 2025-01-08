import random
import string
import pandas as pd

class TestUtils:
    """Helper functions for generating test data and performing reusable operations."""

    @staticmethod
    def get_random_descriptions(model_family, model_details, company, models):
        """
        Generate random strings to test string matching functions.

        Returns:
            tuple: (string_match, string_mismatch1, string_mismatch2)
        """
        sel = models[(models["Company"] == company) & 
                     (models["Model Family"] != model_family) & 
                     (models["Model Details"] != model_details)]
        other_families = sel['Model Family']
        other_details = sel["Model Details"]

        string_match = ''.join(random.choices(string.ascii_uppercase, k=random.randint(0, 5))) + \
                       model_family + \
                       ''.join(random.choices(string.ascii_uppercase, k=random.randint(0, 5))) + \
                       model_details + \
                       ''.join(random.choices(string.ascii_uppercase, k=random.randint(0, 5)))
        
        for others in other_families:
            while others in string_match:
                string_match = ''.join(random.choices(string.ascii_uppercase, k=random.randint(0, 5))) + \
                               model_family + \
                               ''.join(random.choices(string.ascii_uppercase, k=random.randint(0, 5))) + \
                               model_details + \
                               ''.join(random.choices(string.ascii_uppercase, k=random.randint(0, 5)))

        string_mismatch1 = ''.join(random.choices(string.ascii_uppercase, k=15))
        while model_family in string_mismatch1:
            string_mismatch1 = ''.join(random.choices(string.ascii_uppercase, k=15))

        string_mismatch2 = ''.join(random.choices(string.ascii_uppercase, k=15))
        while model_family in string_mismatch2:
            string_mismatch2 = ''.join(random.choices(string.ascii_uppercase, k=15))

        return string_match, string_mismatch1, string_mismatch2

class StringMatchTests:
    """Test cases for validating string matching functionality."""

    @staticmethod
    def test_string_match_true(models, string_match_function):
        """
        Validate that the string match function correctly identifies matches.
        """
        models[["String Match", "String Mismatch 1", "String Mismatch 2"]] = models.apply(
            lambda row: pd.Series(TestUtils.get_random_descriptions(row["Model Family"], 
                                                                    row["Model Details"], 
                                                                    row["Company"], 
                                                                    models)), 
            axis=1)
        
        models[["results_match", "_"]] = models.apply(
            lambda row: pd.Series(string_match_function(row["String Match"], row["Company"])), 
            axis=1)
        models.drop("_", axis=1, inplace=True)

        assert "Unknown_Model" not in models["results_match"]

    @staticmethod
    def test_string_match_false1(models, string_match_function):
        """
        Validate that the string match function correctly identifies mismatches.
        """
        models[["String Match", "String Mismatch 1", "String Mismatch 2"]] = models.apply(
            lambda row: pd.Series(TestUtils.get_random_descriptions(row["Model Family"], 
                                                                    row["Model Details"], 
                                                                    row["Company"], 
                                                                    models)), 
            axis=1)
        
        models[["results_mismatch1", "_"]] = models.apply(
            lambda row: pd.Series(string_match_function(row["String Mismatch 1"], row["Company"])), 
            axis=1)
        models.drop("_", axis=1, inplace=True)

        assert models["results_mismatch1"].str.contains("Unknown_Model").sum() == len(models)

    @staticmethod
    def test_string_match_false2(models, string_match_function):
        """
        Validate that the string match function correctly identifies mismatches.
        """
        models[["String Match", "String Mismatch 1", "String Mismatch 2"]] = models.apply(
            lambda row: pd.Series(TestUtils.get_random_descriptions(row["Model Family"], 
                                                                    row["Model Details"], 
                                                                    row["Company"], 
                                                                    models)), 
            axis=1)
        
        models[["results_mismatch2", "_"]] = models.apply(
            lambda row: pd.Series(string_match_function(row["String Mismatch 2"], row["Company"])), 
            axis=1)
        models.drop("_", axis=1, inplace=True)

        assert models["results_mismatch2"].str.contains("Unknown_Model").sum() == len(models)

class DataDistributionTests:
    """Test cases for validating data distribution over time."""

    @staticmethod
    def test_distribution(new_data, old_data):
        """
        Validate data distribution between old and new datasets and identify outliers.
        """
        if old_data.empty:
            old_data = new_data.copy()
        
        four_months_ago = old_data['Date'].max() - pd.DateOffset(months=3)
        old_data = old_data[old_data['Date'] > four_months_ago]

        previous_grouped = old_data.groupby(["Competitor", "models", "Date"]).agg(
            Quantity_sum=("Quantity", "sum"),
            Total_Dollar_Amount_sum=("Total_Dollar_Amount", "sum"))
        
        current_grouped = new_data.groupby(["Competitor", "models", "Date"]).agg(
            Quantity_sum=("Quantity", "sum"),
            Total_Dollar_Amount_sum=("Total_Dollar_Amount", "sum"))

        previous_grouped = previous_grouped.groupby(["Competitor", "models"]).agg(
            Quantity_mean=('Quantity_sum', 'mean'),
            Quantity_std=('Quantity_sum', 'std'),
            Total_Dollar_Amount_mean=('Total_Dollar_Amount_sum', 'mean'),
            Total_Dollar_Amount_std=('Total_Dollar_Amount_sum', 'std')
        ).reset_index()

        current_grouped = current_grouped.groupby(["Competitor", "models"]).agg(
            Quantity_mean=('Quantity_sum', 'mean'),
            Total_Dollar_Amount_mean=('Total_Dollar_Amount_sum', 'mean')
        ).reset_index()

        merged = pd.merge(previous_grouped, current_grouped, on=['Competitor', 'models'], suffixes=('_old', '_new'))

        outliers = merged[
            (merged['Quantity_mean_new'] > merged['Quantity_mean_old'] + merged['Quantity_std']) |
            (merged['Quantity_mean_new'] < merged['Quantity_mean_old'] - merged['Quantity_std']) |
            (merged['Total_Dollar_Amount_mean_new'] > merged['Total_Dollar_Amount_mean_old'] + merged['Total_Dollar_Amount_std']) |
            (merged['Total_Dollar_Amount_mean_new'] < merged['Total_Dollar_Amount_mean_old'] - merged['Total_Dollar_Amount_std'])
        ]

        outliers.rename(columns={
            'Quantity_std': 'Quantity_std_old',
            'Total_Dollar_Amount_std': 'Total_Dollar_Amount_std_old'},
            inplace=True)

        if not outliers.empty:
            print("The following model(s) mean(s) from the last month are outside its standard deviation(s) from the preceding three months:")
            print(outliers[[
                'Competitor', 'models', 'Quantity_mean_new', 'Quantity_mean_old', 
                'Quantity_std_old', 'Total_Dollar_Amount_mean_new', 
                'Total_Dollar_Amount_mean_old', 'Total_Dollar_Amount_std_old'
            ]])
        else:
            print("No model distribution deviations detected")
