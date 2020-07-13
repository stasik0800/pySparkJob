from pyspark.sql.types import *
import os

def createDir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
    return dir

def getDFSparkSchema():
    return StructType([
        StructField("url", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("type", StringType()),
        StructField("sessionid", IntegerType())])




# app config
# os.environ['HADOOP_HOME'] = "C:\\winutils\\hadoop-2.2.0"
sparkAppName = "Elementor"
encoding = "UTF-8"

# data logic
patterns = ['/search-products', '/display-product/1', '/buy-product']
sessionIndecator = 'start_session'
baseTblName ='baseTbl'
analyticsDf= 'df'


# out files location
projectLocation = os.path.dirname(os.path.abspath(__file__))
errorlogOut = createDir(os.path.join(projectLocation,'out\\errorlog'))
resultsOut = createDir(os.path.join(projectLocation, 'out\\results'))
datalocation = os.path.join(projectLocation, 'data\\StructuredStreamingText.txt')
csvFormat = '.csv'



