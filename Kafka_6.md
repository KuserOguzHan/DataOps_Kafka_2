### 1. Qestion

- Using Python Kafka do the following tasks:
- Produce the names of Turkey's geographical regions to a topic you specify, using the numbers you specify at the beginning of each of them as keys. 
- For example, 1 Marmara, 2 Aegean.
- With the Consumer, print the key, value, partition, timestamp information as following example.

```
Key: 1, Value: Marmara, Partition: 0, TS: 1613224639352 
Key: 4, Value: İç Anadolu, Partition: 1, TS: 1613224654849 
Key: 3, Value: Akdeniz, Partition: 2, TS: 1613224661486 
Key: 2, Value: Ege, Partition: 2, TS: 1613224667044
```

### 2. Docker-Compose Up

- If docker-compose is not run, you should track following steps.
- You should delete kafka1, kafka2, kafka3 and build docker-compose again.
- Chechk week.2.1.md 

```
[train@trainvm ~]$ cd dataops7/kafka/zookeeperless_kafka/
```

```
[train@trainvm zookeeperless_kafka]$ docker-compose ps
```

```
[train@trainvm zookeeperless_kafka]$ sudo systemctl start docker
```

```
[train@trainvm zookeeperless_kafka]$ sudo systemctl status docker
```

```
[train@trainvm zookeeperless_kafka]$ docker-compose up -d
```


### 3. Creata Topic with Admin_client.py in Pycharm

- Open pycharm editor and create new project and admin.client

```
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
import time

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092', 'localhost:9292'],
                                client_id='dataops_client')

# List topics
print("Created topics", admin_client.list_topics())

# Create a topic
try:
    homework1 = NewTopic(name='homework1', num_partitions=2, replication_factor=2)

    admin_client.create_topics(new_topics=[homework1])
except:
    print("Topics are already exist.")


# List topics
time.sleep(2)
print("After create topics", admin_client.list_topics())
```

```
[train@trainvm]$ kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 4. Create Producer

```
from kafka import KafkaProducer
import time


my_producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9292', 'localhost:9392'],
                           client_id='my_producer')

regions = ['Ic Anadolu','Doğu Anadolu','Karadeniz', 'Akdeniz','Marmara','Ege','Güneydogu Anadolu']


for i, val in enumerate(regions):
    my_producer.send(topic='homework1',
                 key=f'{i+1}'.encode("utf-8"),
                 value=f'{val}'.encode("utf-8"))


my_producer.close()
```
