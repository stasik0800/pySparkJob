from pyspark.sql.types import *
import os

# app config
os.environ['HADOOP_HOME'] = "C:\\winutils\\hadoop-2.2.0"
sparkAppName = "Elementor"
encoding = "UTF-8"

# data logic
patterns = ['/search-products', '/display-product/1', '/buy-product']
sessionIndecator = 'start_session'
baseTblName ='baseTbl'
analyticsDf= 'df'



# out files location
projectLocation = os.path.dirname(os.path.abspath(__file__))
errorlogOut = os.path.join(projectLocation,'out\\errorlog')
resultsOut = os.path.join(projectLocation, 'out\\results')
datalocation = os.path.join(projectLocation, 'data\\StructuredStreamingText.txt')
csvFormat = '.csv'


def getDFSparkSchema():
    return StructType([
        StructField("url", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("type", StringType()),
        StructField("sessionid", IntegerType())])
