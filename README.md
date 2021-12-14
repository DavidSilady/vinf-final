# vinf-final

#### Build & run

```
git clone https://github.com/DavidSilady/vinf-final.git
```

##### Processing

```
cd ./indexing/processing

./build.sh

./run.sh
```

Bash container opens:

```
cd /data

# to run link extractor:
./run_link_extractor.sh

# to run sentence extractor:
./run_sentence_extractor.sh

# to exit:
exit
```

To stop containers:
```
./stop.sh
```

##### Indexing

```
cd ./indexing

./build.sh

./run.sh
```

Jupyter notebook opens - run all the nodes or play around

##### Searching

```
./build.sh

./run.sh
```
