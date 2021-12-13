# Imports

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, MapType, StructType
from pyspark import StorageLevel
import pandas as pd
import re
import time
import pandas as pd
import pyspark.sql.functions as sqlf
from pyspark.sql.functions import udf, explode, col, when, array, lower, collect_list
from pyspark.ml.feature import RegexTokenizer
from random import randint


@udf(returnType=ArrayType(StringType()))
def tokenize_sentence(sentence):
    string = str(sentence)
    results = re.findall(r'[^\s!,.?":;]+', string)
    return results


@udf(returnType=ArrayType(StringType()))
def tokenize_into_sentences(text):
    sentences = []
    sentence_tuples =  re.findall(r'((\s|^)\'*[A-Z].+?[.!?])(?=\s+\S*[A-Z]|$)', text)
    for tuple in sentence_tuples:
        sentences.append(clear_sentence(tuple[0]))
    return sentences


def is_valid_sentence(sentence):
    if re.search(r'(\$[0-9])|([\[\]\(\)\{\}\|])', sentence):
        return False
    return True


def clear_sentence(original):
    # unwanted markdown
    processed = re.sub(r'(\'\'\'?)|(^\s)|(<.+?>)|(</.+?>)|(\\n)|(\\xa)', '', original)
    # links
    processed = re.sub(r'(\[\[[^]]*\|)|(]])|(\[\[)', '', processed)
    
    if is_valid_sentence(processed):
        return processed
    
    return None


@udf(returnType=StringType())
def clear_text(text):
    string = str(text)
    return re.sub(r'(<ref.+?/(ref)?>)|(<!--.+?-->)|(\s?(\(([^()])*\)))', '', string)


@udf(returnType=StringType())
def array_to_string(my_list):
    if my_list is not None:
        return '[' + ','.join([str(elem) for elem in my_list]) + ']'
    else:
        return ''


def main():
    # Spark Init
    start_time = time.time()
    spark = SparkSession.builder.master("spark://sparkmaster:7077").appName("sentence_handler").getOrCreate()
    sc = spark.sparkContext

    # extract text tags from xml
    initial_df = spark.read.format('xml').options(rowTag='page').load('./input/wiki.xml')
    df = initial_df.selectExpr("revision.text._VALUE as text")

    # clean text
    df = df.select(clear_text("text").alias("clean_text"))

    # tokenize text into sentences
    sdf = df.select("clean_text", tokenize_into_sentences("clean_text").alias("sentences"))
    sentences = sdf.select(explode("sentences").alias("sentence"))

    # drop None entries
    sentences = sentences.na.drop()
    sentences = sentences.dropDuplicates()

    # FOLLOWING COMMENTED CODE WAS AN ATTEMPT TO JOIN LINK DICTIONARY WITH SENTENCES
    # Failed due to skew and complex spark configuration requirements

    # tokenize sentences into words
    # tokenized = sentences.select("sentence", tokenize_sentence("sentence").alias("tokens"))
    # tokenized = tokenized.select("sentence", explode("tokens").alias("token"))

    #  # load link data
    # schema = StructType() \
    #     .add("lemma", StringType(), True) \
    #     .add("form", StringType(), True) \
    #     .add("postfix", StringType(), True)
    # dictionary = spark.read.format("csv") \
    #     .option("header", False) \
    #     .schema(schema) \
    #     .load("./links.csv")

    # transform to lower for comparisons
    # tokenized = tokenized.withColumn('token', lower(col('token')))
    # dictionary = dictionary.withColumn('lemma', lower(col('lemma')))
    # dictionary = dictionary.withColumn('form', lower(col('form')))

    # join dictionary and sentences
    # tokenized = tokenized.repartition(2000)
    # dictionary = dictionary.repartition(1000)

    # grouped = tokenized.groupBy("token") \
    #     .agg(collect_list("sentence").alias("examples"))

    # joined = grouped.join(dictionary, tokenized.token == dictionary.form, how="right")
    # final = tokenized.join(dictionary, tokenized.token == dictionary.form, how="right")

    # final = tokenized_t.leftOuterJoin(dictionary_t)
    # final.count()
    # grouped = joined.groupBy("form").agg(collect_list("sentence").alias("examples"))
    # final = grouped.join(joined, "form")
    # final = grouped.drop("sentence", "form")
    # final = final.dropDuplicates()

    # output as csv
    # joined = joined.withColumn("examples", array_to_string(col("examples")))
    sentences.write.mode("overwrite").csv('./sentences')


if __name__ == '__main__':
    main()
