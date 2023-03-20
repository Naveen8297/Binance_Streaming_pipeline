
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import os
import time
import csv
import pandas as pd
import yaml

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.6 pyspark-shell'

with open("config.yaml") as f:
   config = yaml.safe_load(f)

symbol = config['symbol']
half_life = config['half_life']
sampling_period = config['sampling_period']
output_folder =  config['output_folder']  #f"./tmp0"
samples_per_file = 60 

state_dict = {}

#for symbol in symbols:
state_dict[symbol] = (0, 0, 0)   # Initial state theta_0

file_index = {}  

file_index[symbol] = 0  



output_data = {'timestamp': [], 'bid_price': [], 'ask_price': []} 

def parse_data(json_data):  # Parse the data received
    data = json.loads(json_data)
    #print(data)
    
    return (data['T'], float(data['b']), float(data['a']))

def update_state(current_values, previous_state, half_life):
	# Calculate decay factor and updated bid/ask price
    decay_factor = 2 ** ((previous_state[0] - current_values[0]) / half_life) 
    
    smoothed_bid = previous_state[1] * decay_factor + current_values[1]
    smoothed_ask = previous_state[2] * decay_factor + current_values[2]
    return (current_values[0], smoothed_bid, smoothed_ask)
    #return (smoothed_bid, smoothed_ask)

# function to write the output to a CSV file
def write_output_file(symbol, file_index, o_data, sample_time):
    # Write only samples_per_file records in one file
    start_index = file_index * samples_per_file
    end_index = (file_index + 1) * samples_per_file
    output_df = pd.DataFrame({
        'timestamp': o_data['timestamp'][start_index:end_index],
        'bid_price': o_data['bid_price'][start_index:end_index],
        'ask_price': o_data['ask_price'][start_index:end_index]
    })
    output_df['timestamp'] = output_df['timestamp'].replace(list(output_df['timestamp']), sample_time)  # Replace with sample_time (end of window)
    output_filename = f"{output_folder}/symbol={symbol}/{file_index+1:06d}.csv"
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    output_df.to_csv(output_filename, index=False)


def output_data_func(time, rdd, output_folder, output_data):

    if not rdd.isEmpty():
        sample_time = int(round(time * 1000))  # Sampling time
        
        #symbol_rdd down
        symbol_data = rdd.map(lambda x: update_state(x, state_dict[symbol], half_life)).collect()
        for sample in symbol_data:
            output_data['timestamp'].append(sample[0])
            output_data['bid_price'].append(sample[1])
            output_data['ask_price'].append(sample[2])
        #print(symbol_data, len(symbol_data))
        if len(output_data['timestamp']) >= samples_per_file:
            last_timestamp = output_data['timestamp'][-1]   # Most recent timestamp
            if sample_time - last_timestamp >= sampling_period:
                write_output_file(symbol, file_index[symbol], output_data, sample_time)   
                file_index[symbol] += 1
                output_data = {'timestamp': [], 'bid_price': [], 'ask_price': []}

        

sc = SparkContext(appName="KafkaStreaming")
ssc = StreamingContext(sc, int(sampling_period/1000))

brokers = "localhost:9092"
topic = "stream"   # Replace with your kafka topic

kafka_params = {"metadata.broker.list": brokers} 

directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams=kafka_params)
stream = directKafkaStream.map(lambda v: v[1].replace("\'", "\""))   # Format the escape characters before parsing
parsed = stream.map(parse_data)
#parsed.pprint()
parsed.foreachRDD(lambda rdd: output_data_func(time.time(), rdd, output_folder, output_data))


ssc.start()
ssc.awaitTermination()


