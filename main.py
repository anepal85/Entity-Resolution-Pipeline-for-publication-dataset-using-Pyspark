from pyspark.sql.session import SparkSession 
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

from pyspark.sql.functions import split

# Define schema 
    # Name of the paper -> paper title 
    # author names 
    # yop 
    # publication venue 
    # indexid - paper id
    # the id of references of this paper 
    # abstract  

# Task 1.1
#  Collect all the publications published between 
# 1995 to 2004 in VLDB and SIGMOD venues.
def process_academic_papers(file_path, num_publications=10):
    spark = SparkSession.builder.appName("AcademicPapers").getOrCreate()

    publication_schema = StructType([
        StructField("title", StringType(), True),
        StructField("authors", ArrayType(StringType()), True),
        StructField("year", IntegerType(), True),
        StructField("venue", StringType(), True),
        StructField("id", StringType(), True),
        # StructField("n_citation", IntegerType(), True),
        # StructField("references", ArrayType(StringType()), True),
        # StructField("abstract", StringType(), True)
    ])
    
    data_frame = spark.read.schema(publication_schema).format("txt").option("header", False).load(file_path)
    # raw_data = spark.read.text(file_path).limit(num_publications)

    # json_data = raw_data.select(from_json(raw_data.value, publication_schema).alias("data")).select("data.*")

    data_frame.show(truncate=False)

if __name__ == "__main__":
    input_file_path = 'data/dblp.v8/dblp.txt'
    process_academic_papers(input_file_path, num_publications=10)