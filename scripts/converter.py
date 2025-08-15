import pandas as pd

def excel_csv_converter(input_file: str, output_file: str):
    """
    Convert Excel to CSV
    :param input_file:
    :param output_file:
    :return:
    """
    pandas_dataframe = pd.read_excel(input_file, sheet_name='Worksheet')
    pandas_dataframe = pandas_dataframe.replace(
        "\n", "",
        regex=True
    ).replace(
        "\r", "",
        regex=True
    ).replace(
        "\r\n", "",
        regex=True
    )
    pandas_dataframe.to_csv(output_file, index=False)

if __name__ == '__main__':
    excel_csv_converter(
        input_file="source_data/Customer.xlsx",
        output_file="source_data/Customers.csv"
    )
