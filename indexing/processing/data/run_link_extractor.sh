#!/bin/bash

/opt/bitnami/spark/bin/spark-submit --master spark://sparkmaster:7077 ./link_extractor.py && \
echo "Cleaning original file" && \
rm ./links.csv || true && \
echo "Cleaning .crc files" && \
rm ./links/.*.crc && \
echo "Merging into links.csv" && \
cat ./links/*.csv > ./output/links.csv && \
echo "Cleaning generated .csv files" && \
rm ./links/*.csv && \
echo Done