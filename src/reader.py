from spark_context import spark

class DataSourceReader:
    
    def __init__(self, file_path):
        self.file_path = file_path

    def get_data_frame(self):

        return ValueError("Class not implemented")
    

class CSVDataSourceReader(DataSourceReader):

    def get_data_frame(self):

        return (
            spark
            .read
            .format("csv")
            .options(header='True')
            .load(self.file_path)
        )


class ParquetDataSourceReader(DataSourceReader):

    def get_data_frame(self):

        return (
            spark
            .read
            .format("parquet")
            .load(self.file_path)
        )


class DeltaDataSourceReader(DataSourceReader):

    def get_data_frame(self):

        return (
            spark
            .read
            .format("delta")
            .load(self.file_path)
        )

def get_data_source(data_type, file_path):

    if data_type == "csv":
        return CSVDataSourceReader(file_path)
    elif data_type == "parquet":
        return ParquetDataSourceReader(file_path)
    elif data_type == "delta":
        return DeltaDataSourceReader(file_path)
    else:
        raise ValueError("Given datatype: {} is not supported by app".format(data_type))