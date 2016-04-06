
scp -r stat1002.eqiad.wmnet:/home/ellery/readers/data/click_traces/rs3v2/year=2016/month=3/day=1 ~/readers/data/rs3v2/year=2016/month=3

scp -r stat1002.eqiad.wmnet:/home/ellery/readers/data/click_traces/rs3v2 ~/readers/data



python create_hive_traces.py \
--start 2016-03-01 \
--stop 2016-03-08 \
--db traces \
--table rs3v2 \
--priority



python create_hive_traces.py \
--start 2016-01-15 \
--stop 2016-03-15 \
--db traces \
--table a2v

spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 4 \
    --queue priority \
hash_trace_ips.py \
    --start 2016-01-17 \
    --stop 2016-03-15 \
    --input_dir /user/hive/warehouse/traces.db/a2v \
    --output_dir /user/ellery/readers/data/hashed_traces/a2v \
    --key 

spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 4 \
    --queue priority \
a2v_preprocess.py \
    --input_dir /user/ellery/readers/data/hashed_traces/a2v/\*/\*/\*/\* \
    --output_file a2v_sessions 

python /home/ellery/readers/src/traces/a2v_training.py \
    --in_file /user/ellery/readers/data/a2v/sessions/a2v_sessions_dir \
    --out_file /home/ellery/readers/data/a2v/models/a2v_sessions



    