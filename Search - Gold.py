# Databricks notebook source
!pip install langchain[all]
!pip3 install Langchain[FAISS]
!pip install faiss-cpu
!pip install langchain-community
!pip install -U sentence-transformers

# COMMAND ----------

from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain import FAISS

# COMMAND ----------

embedding=HuggingFaceEmbeddings(model_name='all-MiniLM-L6-v2')

# COMMAND ----------

faiss_db=FAISS.load_local('/FileStore/tables/vectordb/llm_imdb/',embeddings=embedding,allow_dangerous_deserialization=True)

# COMMAND ----------

docs=faiss_db.similarity_search_with_score('dangal is action biography drama directed by nitesh tiwari which is former wrestler mahavir singh phogat and his two wrestler daughters struggle towards glory at the commonwealth games in the face of',5)
docs[0]

# COMMAND ----------

docs[0]

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
# Extracting data from the tuple
document = docs[0][0]
score = float(docs[0][1])

# Preparing data to be saved
data_to_save = [(document.metadata['id'], document.metadata['Series_Title'], document.metadata['Genre'],
                 document.metadata['Director'], document.metadata['Overview'], document.page_content, score)]

# Define schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("Series_Title", StringType(), True),
    StructField("Genre", StringType(), True),
    StructField("Director", StringType(), True),
    StructField("Overview", StringType(), True),
    StructField("Page_Content", StringType(), True),
    StructField("Score", DoubleType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data_to_save, schema)

# Show DataFrame
display(df)
df.write.mode('overwrite').saveAsTable('llm_gold.imdb_gold')

