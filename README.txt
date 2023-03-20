-----Requirements-----
Python 3.7.7, Apache Spark (2.4.6) 
Apache Kafka along with Zookeeper broker (Windows: https://www.goavega.com/blog/install-apache-kafka-on-windows/,
Ubuntu: https://hevodata.com/blog/how-to-install-kafka-on-ubuntu/,
https://kafka.apache.org/quickstart)
Python libraries: Pyspark (2.4.6), Pyyaml, Websocket

Step-1: Start the zookeeper service
Step-2: Start the Kafka server on port 9092 (default)
Step-3: Setup your Kafka topic (name it "stream" or change code accordingly)

(Detailed instructions for Step-1 to 3 are given in the links above)

Step-4: Install the required Python libraries in your environment
Step-5: In your environment, run receiver.py. The script is for listening to port 9093 and sending the data received to your Kafka topic (server running at 9092).
Step-6: Run connector.py: This script gathers data from the Binance stream and sends it to port 9093.
Step-7: Run pipeline1.py: Data stream is sent from port 9093 to Kafka topic, pipeline1.py consumes data from the Kafka topic and applies required transformations on it. Finally it writes the transformed data in csv files in the required format.
Please note that values of variables fed to the programs can be changed in config.yaml file.

The Kafka topic has been used since directly sending data from connector to receiver and consuming using socketStream() was creating difficulties. The main issue was parsing the data after the pipeline received it. Hence, the Kafka topic approach. Please also note that currently the piepline works for only one symbol at a time. 

Q: Can the application provide a guarantee of “exactly-once”-delivery?

It is difficult to achieve exactly-once delivery. In the connector.py file I use socket.send() method to send data from Binance stream to another port. This method itself breaks the guarantee since it may not send all the data that is given to it. In most use-cases, achieving "at least once" or "at most once" delivery is sufficient, since in a data pipeline there can be multiple stages where there can be data loss due to network/hardware or any other type of failure. 

 