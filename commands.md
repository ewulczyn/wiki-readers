# Data Preparation

## Survey Data
1. dowload survey [responses](https://drive.google.com/open?id=1JD8-knLmnFXVwXxJYx6w9RRmxZWasSLaSKvj85SgrzE) to data/raw_responses.tsv.
2. run `src/analysis/Response Cleaning.ipynb` to get  data/responses.tsv

## Trace Data

1. run `src/traces/create_hive_traces.py`. This creates a table of requests grouped by ip, ua, xff for each day.

```
python create_hive_traces.py \
--start 2016-03-01 \
--stop 2016-03-08 \
--db traces \
--table rs3v2 \
--priority

```
2. run `src/traces/hash_trace_ips.py`. This takes the hive table of requests, hashes the ips using the supplied key, drops xff, and outputs the traces in json in HDFS.

```
spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 4 \
    --executor-memory 10g \
    --executor-cores 4 \
    --queue priority \
hash_trace_ips.py \
    --start 2016-03-01 \
    --stop 2016-03-08 \
    --input_dir /user/hive/warehouse/traces.db/rs3v2 \
    --output_dir /user/ellery/readers/data/hashed_traces/rs3v2 \
    --key
```
3.  run `src/traces/join_traces_and_clicks.py`. This joins 'Yes' click events on the survey widget in EL with the hashed traces and outputs a `join_data.tsv` file for each day.

```
spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 4 \
    --executor-memory 20g \
    --executor-cores 4 \
    --queue priority \
join_traces_and_clicks.py \
    --start 2016-03-01 \
    --stop 2016-03-08 \
    --input_dir /user/ellery/readers/data/hashed_traces/rs3v2 \
    --output_dir /home/ellery/readers/data/click_traces/rs3v2 
```

## Joining Survey Data and Traces
1. copy click traces to local machine

```
scp -r stat1002.eqiad.wmnet:/home/ellery/readers/data/click_traces/rs3v2 ~/readers/data/click_traces/
```

2. run src/traces/Join Survey and Traces.ipynb. This generates repsonses_with_traces.tsv, which contains the survey responses for which we have traces.