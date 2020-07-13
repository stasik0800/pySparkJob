import properties as p
import config, os
from helper import Sparkfn
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


def writeDF2Csv(spark, query):
    try:
        df = spark.sql(query['query'])
        location = os.path.join(query['path'], query['name'])
        df.toPandas().to_csv(location, index=False)

    except Exception as e:
        print(f" check {query}")
        raise e

    print(f"Done  -->  {location}")


def buildBaseDataFrame(spark):
    "This function adding session id over userid and timestamps dependes on start_session  event \
     Bind schema to analytics dataFrame  "

    # read text file from location Dir
    data = spark.read.text(config.datalocation).cache()
    data = data.rdd.map(Sparkfn.splitData).collect()

    data = Sparkfn.addSessionId(data)
    schema = config.getDFSparkSchema()

    # create local baseTbl Dataframe
    baseTblDF = spark.createDataFrame(data, schema)
    baseTblDF.createOrReplaceTempView(config.baseTblName)


def dataFlowExec(spark):
    qurey = p.Quries()

    # prepare base dataframe (staging)
    buildBaseDataFrame(spark)

    # errors out
    writeDF2Csv(spark, qurey.invalidData)

    # preparing DataFrame for analysis
    toAnalysDF = spark.sql(qurey.prepareData['query'])
    toAnalysDF.createOrReplaceTempView(qurey.prepareData['name'])

    # results out
    for resultQueries in qurey.questionsDict:
       writeDF2Csv(spark, resultQueries)


def main():
    sc = SparkContext(appName=config.sparkAppName)
    spark = SQLContext(sc)

    # set custom function
    spark.udf.register("isNumeric", Sparkfn.is_numeric, BooleanType())
    spark.udf.register("getPattern", Sparkfn.getPattern, StringType())
    spark.udf.register("to_timestamp", Sparkfn.to_timestamp, IntegerType())

    dataFlowExec(spark)


if __name__ == "__main__":
    main()
