#!/bin/bash

/opt/bitnami/spark/bin/spark-submit --master spark://sparkmaster:7077 ./sentence_extractor.py && \
echo "Cleaning original file" && \
rm ./sentences.csv || true && \
echo "Cleaning .crc files" && \
rm ./sentences/.*.crc && \
echo "Merging into links.csv" && \
cat ./sentences/*.csv > ./output/sentences.csv && \
echo "Cleaning generated .csv files" && \
rm ./sentences/*.csv && \
echo Done