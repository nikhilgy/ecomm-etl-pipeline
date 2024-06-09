from reader import get_data_source

class Extractor:
    
    def __init__(self):
        pass

    def extract(self):
        pass


class AirPodsAfteriPhoneExtractor(Extractor):

    def extract(self):

        transaction_df = get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()

        customers_df = get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/Customer_Updated.csv"
        ).get_data_frame()

        return {
            "transaction_df" : transaction_df,
            "customers_df" : customers_df
        }
