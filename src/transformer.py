from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, collect_set, array_contains, size, broadcast, datediff, avg

class Transform:

    def __init__(self, input_dfs):
        self.input_dfs = input_dfs

    def transform(self):
        pass


class FirstTransformation(Transform):
    """
    Transformation 1: Get all customers who bought Airpods after buying an iphone

    Ans: Sort all transactions based on transaction date, then form window of partitioned customer_id, 
    in that apply LEAD on product_name as next_product_name and then filter on product_name as iPhone next_product_name as Airpods
    """

    def transform(self):

        transaction_df = self.input_dfs['transaction_df']
        customers_df = self.input_dfs['customers_df']

        windowSpec = Window.partitionBy("customer_id").orderBy("customer_id", "transaction_date")

        """Get all transactions where user has bought Airpods after iPhone"""
        required_transactions = transaction_df.withColumn("next_product_name",lead("product_name").over(windowSpec))
        filtered_transactions = required_transactions.filter((col("product_name") == "iPhone") & (col("next_product_name") == "AirPods"))


        """Join with customers df to get details of customers"""
        result_df = customers_df.alias("customers_df") \
        .join(filtered_transactions, customers_df.customer_id == filtered_transactions.customer_id) \
        .select(
            col("customers_df.customer_id"), 
            col("customers_df.customer_name"), 
            col("customers_df.join_date"), 
            col("customers_df.location")
        )

        return result_df




class SecondTransformation(Transform):
    """
    Transformation 2: Get all customers who bought only AirPods & iPhone

    Ans: Apply Collect List function on product_name column and then check airpods, iphone & size of list 
    """

    def transform(self):

        transaction_df = self.input_dfs['transaction_df']
        customers_df = self.input_dfs['customers_df']

        """Apply collect_set on transaction_df on product_name column"""
        transaction_df = transaction_df \
        .groupBy("customer_id") \
        .agg(collect_set("product_name") \
        .alias("products_bought"))

        """Now check list of products_bought should just have airpods and iphone"""
        filtered_transactions = transaction_df.filter(
            array_contains(col("products_bought"), "AirPods") &
            array_contains(col("products_bought"), "iPhone") &
            (size(col("products_bought")) == 2)
        )
        
        """Join with customers df to get details of customers"""
        result_df = customers_df.alias("customers_df") \
        .join(broadcast(filtered_transactions), customers_df.customer_id == filtered_transactions.customer_id) \
        .select(
            col("customers_df.customer_id"), 
            col("customers_df.customer_name"), 
            col("customers_df.join_date"), 
            col("customers_df.location")
        )
        
        result_df.show()
        return result_df
    
    

class ThirdTransformation(Transform):
    """
    Transformation 3: Get all products bought after their initial purchase

    Ans: Sort transaction on basis of date & customer_id, we'll get next_product_name column 
    using LEAD and then remove rows where next_product_name is null which means not bought 
    after and then group by and collect_set on next_product_name to get all products bought after first purchase
    """

    def transform(self):

        windowSpec = Window.partitionBy("customer_id").orderBy("customer_id", "transaction_date")

        transaction_df = self.input_dfs['transaction_df']
        transaction_df = transaction_df.alias("transaction_df")
        customers_df = self.input_dfs['customers_df']

        """Get all transactions where user has bought Airpods after iPhone"""
        required_transactions = transaction_df.withColumn("next_product_name",lead("product_name").over(windowSpec))

        """Remove transactions where next_product_name is null"""
        required_transactions = required_transactions.filter(
            col("next_product_name") != "null"
        )

        """Apply collect_set on transaction_df on next_product_name column"""
        filtered_transactions = required_transactions \
        .groupBy("customer_id") \
        .agg(collect_set("next_product_name") \
        .alias("products_bought_after_initial_purchase"))

        filtered_transactions = filtered_transactions.alias("filtered_transactions")
        
        filtered_transactions.show()

        """Join with customers df to get details of customers"""
        result_df = customers_df.alias("customers_df") \
        .join(broadcast(filtered_transactions), customers_df.customer_id == filtered_transactions.customer_id) \
        .select(
            col("customers_df.customer_id"), 
            col("customers_df.customer_name"), 
            col("customers_df.join_date"), 
            col("customers_df.location"), 
            col("filtered_transactions.products_bought_after_initial_purchase")
        )
        
        # result_df.show()

        return result_df
    
    

class FourthTransformation(Transform):
    """
    Transformation 4: Determine average time delay between buying an iphone and airpods for customer

    Ans: Get all transactions with iphone purchases and airpods purchases in two seperate dataframes. 
    Now join both of them on customer_id where AirPods_purchase date should be greater than iPhone_purchase date. 
    Now find datediff in new column and then take avrage of it
    """

    def transform(self):
        
        transaction_df = self.input_dfs['transaction_df']
        transaction_df = transaction_df.alias("transaction_df")
        
        """Get dataframe of iPhone purchases"""
        iPhone_purchases_df = transaction_df.filter(col("product_name") == "iPhone") \
        .select(
            col("transaction_id"),
            col("customer_id"), 
            col("product_name"), 
            col("transaction_date").alias("iPhone_date")
        )
        
        iPhone_purchases_df = iPhone_purchases_df.alias("iphone_transactions")
        
        # iPhone_purchases_df.orderBy("customer_id","iPhone_date").show()

        """Get dataframe of Airpods purchases"""
        airpods_purchases_df = transaction_df.filter(col("product_name") == "AirPods") \
        .select(
            col("transaction_id"),
            col("customer_id"), 
            col("product_name"), 
            col("transaction_date").alias("AirPods_date")
        )
        
        airpods_purchases_df = airpods_purchases_df.alias("airpods_transactions")
        # airpods_purchases_df.orderBy("customer_id","AirPods_date").show()

        """Join both iphone_purchase and airpods_purchase transactions"""
        joined_df = iPhone_purchases_df.join(
            broadcast(airpods_purchases_df),
            "customer_id"
        ).filter(
            col("airpods_transactions.AirPods_date") > col("iphone_transactions.iPhone_date")  
        )

        joined_df \
        .withColumn("purchase_diff", datediff(col("AirPods_date"), col("iPhone_date"))) \

        result_df = joined_df \
        .withColumn("purchase_diff", datediff(col("AirPods_date"), col("iPhone_date"))) 
        result_df.show()

        return result_df.select(avg("purchase_diff")).collect()[0][0]

