class DataLoader:
    
    def __init__(self, result_df, load_type, file_path, partition_by=None):
        self.result_df = result_df
        self.load_type = load_type
        self.file_path = file_path
        self.partition_by = partition_by

    def load_data_frame(self):
        pass


class DBFSDataLoader(DataLoader):

    def load_data_frame(self):
        self.result_df \
        .write \
        .format("csv") \
        .mode(self.load_type) \
        .option("header", "true") \
        .save(self.file_path)

        print("Dataframe successfully written to DBFS at path: {}".format(self.file_path))

class DBFSDataLoaderByPartition(DataLoader):

    def load_data_frame(self):
        self.result_df \
        .write \
        .partitionBy(self.partition_by) \
        .mode(self.load_type) \
        .save(self.file_path)

        print("Dataframe successfully written to DBFS at path: {}".format(self.file_path))

class DBFSDataLoaderToDeltaTable(DataLoader):

    def load_data_frame(self):
        self.result_df \
        .write \
        .format("delta") \
        .mode(self.load_type) \
        .saveAsTable(self.file_path)

        print("Dataframe successfully written to DBFS at table path: {}".format(self.file_path))


def load_data(load_to, result_df, load_type, file_path, partition_by=None):

    if load_to == "dbfs":
        return DBFSDataLoader(result_df, load_type, file_path, partition_by)
    elif load_to == "dbfs_with_partition":
        return DBFSDataLoaderByPartition(result_df, load_type, file_path, partition_by)
    elif load_to == "delta_table":
        return DBFSDataLoaderToDeltaTable(result_df, load_type, file_path, partition_by)
    else:
        raise ValueError("Given load to is not supported")
