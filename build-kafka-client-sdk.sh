#!/usr/bin/env bash

# kafkaclient.tgz

# kafkaclient/
#   build-kafka-client-sdk.sh
#   cpp/
#     PyKafkaClient.h
#     PyKafkaClient.cpp
#   java/
#     build.xml
#     PyKafkaProducer.java
#     PyKafkaConsumer.java
#     IConsumeCallback.java
#     BaseConsumeCallback.java
#     jni/
#       com_ipinyou_kafka_PyKafkaProducer.cpp
#       com_ipinyou_kafka_PyKafkaConsumer.cpp

HOME_DIR=/data/users/data-infra
JAVA_PATH=/usr/lib/jvm/java-1.7.0-openjdk.x86_64
ANT_BIN=/sbin/ant

BUILD_TYPE=$1
echo "build type: $BUILD_TYPE"

# 1: cpp only 2: java only 3: both
if [ $BUILD_TYPE -eq 2 ] || [ $BUILD_TYPE -eq 3 ]
then
    $ANT_BIN -version
    if [ $? -ne 0 ]
    then
        echo "/sbin/ant not found. Please install ant first"
        exit 1
    fi
    JAVA_PATH=$2
fi

mkdir -p $HOME_DIR/kafkaclient/cpp

cd $HOME_DIR

######## librdkafka ########

LIBRDKAFKA_URL=https://github.com/edenhill/librdkafka/archive/master.zip
LIBRDKAFKA_DIR=librdkafka-master
LIBRDKAFKA_ZIP="$LIBRDKAFKA_DIR.zip"

rm -rf $LIBRDKAFKA_DIR $LIBRDKAFKA_ZIP

wget --no-check-certificate -O $LIBRDKAFKA_ZIP $LIBRDKAFKA_URL

unzip $LIBRDKAFKA_ZIP

cd $LIBRDKAFKA_DIR

# prefix is "/usr/local"
./configure
make
sudo make install

######## PyKafkaClient ########


cd $HOME_DIR/kafkaclient/cpp

g++ -g -W -Wall -c -fPIC -I/usr/local/include/librdkafka -o PyKafkaClient.o PyKafkaClient.cpp
#g++ -g -W -Wall -c -fPIC -I/usr/local/include/librdkafka -pg -o PyKafkaClient.o PyKafkaClient.cpp
g++ -shared -L/usr/local/lib -L/usr/lib64 -o $HOME_DIR/kafkaclient/cpp/libpykafkaclient.so PyKafkaClient.o -lrdkafka -lz -lpthread -lrt
#g++ -shared -L/usr/local/lib -L/usr/lib64 -pg -o $HOME_DIR/kafkaclient/cpp/libpykafkaclient.so PyKafkaClient.o -lrdkafka -lz -lpthread -lrt
g++ -shared -o $HOME_DIR/kafkaclient/cpp/libpykafkaclient.a PyKafkaClient.o /usr/local/lib/librdkafka.a -lz -lpthread -lrt
#g++ -shared -pg -o $HOME_DIR/kafkaclient/cpp/libpykafkaclient.a PyKafkaClient.o /usr/local/lib/librdkafka.a -lz -lpthread -lrt

if [ $BUILD_TYPE -eq 1 ]
then
    exit 0
fi

mkdir -p $HOME_DIR/kafkaclient/java/jni

cd $HOME_DIR/kafkaclient/java

$JAVA_PATH/bin/javac -g -d . PyKafkaProducer.java
$JAVA_PATH/bin/javac -g -d . IConsumeCallback.java
$JAVA_PATH/bin/javac -g -d . BaseConsumeCallback.java
$JAVA_PATH/bin/javac -g -d . PyKafkaConsumer.java

$JAVA_PATH/bin/javah -verbose -d jni -jni com.ipinyou.kafka.PyKafkaProducer
$JAVA_PATH/bin/javah -verbose -d jni -jni com.ipinyou.kafka.PyKafkaConsumer

g++ -g -W -Wall -I./jni -I../cpp -I$JAVA_PATH/include -I$JAVA_PATH/include/linux -I/usr/local/include/librdkafka -fPIC -o jni/PyKafkaProducerJNI.o -c jni/com_ipinyou_kafka_PyKafkaProducer.cpp
g++ -shared -o $HOME_DIR/kafkaclient/java/jni/libpykafkaproducerjni.so jni/PyKafkaProducerJNI.o $HOME_DIR/kafkaclient//cpp/libpykafkaclient.a

g++ -g -W -Wall -I./jni -I../cpp -I$JAVA_PATH/include -I$JAVA_PATH/include/linux -I/usr/local/include/librdkafka -fPIC -o jni/PyKafkaConsumerJNI.o -c jni/com_ipinyou_kafka_PyKafkaConsumer.cpp
g++ -shared -o $HOME_DIR/kafkaclient/java/jni/libpykafkaconsumerjni.so jni/PyKafkaConsumerJNI.o ../cpp/libpykafkaclient.a

$ANT_BIN
