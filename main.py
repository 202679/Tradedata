from data_load import load_data
from data_transform import transform_data
from data_export import export_data
from Tests import run_tests

def main():
    # Load Data
    data = load_data('C:/Tradedata_output/data.csv')
    
    # Transform Data
    transformed_data = transform_data(data)

    # Run Tests on Transformed Data
    run_tests(data)
    
    # Export Transformed Data
    export_data(transformed_data, 'data/output.csv')

if __name__ == "__main__":
    main()