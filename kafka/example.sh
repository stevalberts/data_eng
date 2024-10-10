# Download and extract Kafka
wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz

tar -xzf kafka_2.13-3.8.0.tgz

# Configure KRaft and start server

# Navigate to the kafka_2.13-3.8.0 directory.
cd kafka_2.13-3.8.0

# Generate a cluster UUID that will uniquely identify the Kafka cluster.
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# This cluster id will be used by the KRaft controller.
# KRaft requires the log directories to be configured. Run the following command to configure the log directories passing the cluster ID.
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Now that KRaft is configured, you can start the Kafka server by running the following command.
bin/kafka-server-start.sh config/kraft/server.properties

# Create a topic and start producer
# You need to create a topic before you can start to post messages.
# Start a new terminal and change to the kafka_2.13-3.8.0 directory.
cd kafka_2.13-3.8.0

# To create a topic named news, run the command below.
bin/kafka-topics.sh --create --topic news --bootstrap-server localhost:9092

# You will see the message: Created topic news.
# You need a producer to send messages to Kafka. Run the command below to start a producer.
bin/kafka-console-producer.sh   --bootstrap-server localhost:9092   --topic news

# After the producer starts, and you get the '>' prompt, type any text message and press enter. 
# Or you can copy the text below and paste. The below text sends three messages to Kafka.


# Start Consumer
# You need a consumer to read messages from Kafka.

# Start a new terminal and change to the kafka_2.13-3.8.0 directory.
cd kafka_2.13-3.8.0

# Run the command below to listen to the messages in the topic news.
bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic news   --from-beginning

# You should see all the messages you sent from the producer appear here.
# You can go back to the producer terminal and type some more messages,
# one message per line, and you will see them appear here.

# Explore Kafka directories
# Kafka uses the /tmp//tmp/kraft-combined-logs directory to store the messages.
cd kafka_2.13-3.8.0

# Explore the root directory of the server.
ls


# Notice there is a tmp directory. The kraft-combine-logs inside the tmp directory contains all the logs.
# To check the logs generated for the topic news run the following command:
ls /tmp/kraft-combined-logs/news-0

# Note: All messages are stored in the news-0 directory under the /tmp/kraft-combined-logs directory.

# To stop use CTRL+C

