# Imports

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, MapType
import pandas as pd
import re
import time
import pandas as pd
import pyspark.sql.functions as sqlf
from pyspark.sql.functions import udf, explode, col, when, array, lower, collect_list
from pyspark.ml.feature import RegexTokenizer


def is_dict_pair_valid(dict_pair):
    # If there is not change, skip
    if dict_pair['base'] == dict_pair['form']:
        return False
    # If the starting letter differs, the pair is not valid
    if not dict_pair['base'][0] == dict_pair['form'][0]:
        return False
    # If the number of words differ, the pair is not valid
    if not len(dict_pair['base'].split(' ')) == len(dict_pair['form'].split(' ')):
        return False
    return True


@udf(returnType=ArrayType(MapType(StringType(), StringType())))
def extract_links(text):
    dict_results = []
    results = re.findall(r'\[\[[A-Za-z0-9.]+?\|.+?]]', text)
    
    # Variant without postfix
    if results:
        for result in results:
            dict_result = {
                'base': re.findall(r'\[\[(.+?)\|', result)[0],
                'form': re.findall(r'\|(.+?)]]', result)[0],
                'postfix': ''
            }
            if is_dict_pair_valid(dict_result):
                dict_results.append(dict_result)
    
    # Variant with postfix
    if not results:
        results = re.findall(r'\[\[[A-Za-z0-9.]+?]][a-z]+?\s', text)
        if results:
            for result in results:
                dict_result = {
                    'base': re.findall(r'\[\[(.+?)]]', result)[0],
                    'postfix': re.findall(r'\[\[.+?]](.*)\s', result)[0]
                }
                dict_result['form'] = dict_result['base'] + dict_result['postfix']
                dict_results.append(dict_result)
                
    return dict_results


@udf(returnType=StringType())
def clear_text(text):
    string = str(text)
    return re.sub(r'(<ref.+?/(ref)?>)|(<!--.+?-->)|(\s?(\(([^()])*\)))', '', string)


def main():
    # Spark Init
    start_time = time.time()
    spark = SparkSession.builder.master("spark://sparkmaster:7077").appName("link_extractor").getOrCreate()
    sc = spark.sparkContext

    # extract text tags from xml
    initial_df = spark.read.format('xml').options(rowTag='page').load('./input/wiki.xml')
    df = initial_df.selectExpr("revision.text._VALUE as text")

    # clean text
    df = df.select(clear_text("text").alias("clean_text"))

    # extract links to dict
    edf = df.select(extract_links("clean_text").alias("links"))
    edf = edf.select(explode("links").alias("link"))
    exprs = [col("link").getItem(k).alias(k) for k in ["base", "form", "postfix"]]
    edf = edf.select(*exprs)

    # transform empty poxtfix to None
    edf = edf.withColumn('postfix', when(col('postfix') == '', None).otherwise(col('postfix')))
    
    # remove duplicates
    # note: some duplicates may still occur if one of them has a postfix and other does not
    dictionary = edf.dropDuplicates()

    dictionary.write.mode("overwrite").csv("./links")



if __name__ == '__main__':
    main()