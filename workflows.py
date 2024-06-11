from src.extractor import AirPodsAfteriPhoneExtractor
from src.transformer import FirstTransformation, SecondTransformation, ThirdTransformation, FourthTransformation
from src.loader import load_data

class FirstWorkflow:
    """
    Get all customers who bought Airpods after buying an iphone
    """

    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract Data from sources
        input_dfs = AirPodsAfteriPhoneExtractor().extract()

        # Step 2: Perform transformation on extracted dataframes
        result_df = FirstTransformation(input_dfs).transform()

        # Step 3: Write to dbfs
        load_data(
            load_to="dbfs",
            result_df=result_df,
            load_type="overwrite",
            file_path="dbfs:/FileStore/tables/users_bought_airpods_after_iphone.csv"
        ).load_data_frame()


class SecondWorkflow:
    """
    Get all customers who bought only AirPods & iPhone
    """    
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract Data from sources
        input_dfs = AirPodsAfteriPhoneExtractor().extract()

        # Step 2: Perform transformation on extracted dataframes
        result_df = SecondTransformation(input_dfs).transform()

        # Step 3: Write to dbfs
        load_data(
            load_to="dbfs_with_partition",
            result_df=result_df,
            load_type="overwrite",
            file_path="dbfs:/FileStore/tables/only_airpods_and_iphone",
            partition_by="location"
        ).load_data_frame()

        # Load to Delta Table
        load_data(
            load_to="delta_table",
            result_df=result_df,
            load_type="overwrite",
            file_path="default.only_airpods_and_iphone"
        ).load_data_frame()


class ThirdWorkflow:
    """
    Get all products bought after their initial purchase
    """
    
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract Data from sources
        input_dfs = AirPodsAfteriPhoneExtractor().extract()

        # Step 2: Perform transformation on extracted dataframes
        result_df = ThirdTransformation(input_dfs).transform()

        # # Step 3: Write to dbfs
        # Load to Delta Table
        load_data(
            load_to="delta_table",
            result_df=result_df,
            load_type="overwrite",
            file_path="default.products_bought_after_initial_purchase"
        ).load_data_frame()

class FourthWorkflow:
    """
    Determine average time delay between buying an iphone and airpods for customer
    """
    
    def __init__(self):
        pass
    
    def runner(self):
        
        # Step 1: Extract Data from sources
        input_dfs = AirPodsAfteriPhoneExtractor().extract()

        # Step 2: Perform transformation on extracted dataframes
        result = FourthTransformation(input_dfs).transform()
            
        print("A customer buys Airpods after iPhone on average after: {} days.".format(result))

