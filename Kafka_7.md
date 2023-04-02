### 1. Question

- Truncate topic1.
- Produce iris.csv using data-generator to topic1.
- Build a python consumer;
	- Comsume from topic1. 
	- Write the message content, topic name, partition number of each flower type in a separate file with its own name (`/tmp/kafka_out/<species_name_out.txt`>).
	- Write messages that do not belong to any of the three flower types in the `/tmp/kafka_out/other_out.txt` file.

Example result file tree: 

```
tree /tmp/kafka_out/
/tmp/kafka_out/
├── other_out.txt
├── setosa_out.txt
├── versicolor_out.txt
└── virginica_out.txt
```

Example file content
```
 head /tmp/kafka_out/setosa_out.txt
topic1|2|0|0|5.1,3.5,1.4,0.2,Iris-setosa
topic1|2|1|2|4.7,3.2,1.3,0.2,Iris-setosa
topic1|2|2|3|4.6,3.1,1.5,0.2,Iris-setosa
topic1|2|3|9|4.9,3.1,1.5,0.1,Iris-setosa
topic1|2|4|16|5.4,3.9,1.3,0.4,Iris-setosa
topic1|2|5|29|4.7,3.2,1.6,0.2,Iris-setosa
topic1|2|6|32|5.2,4.1,1.5,0.1,Iris-setosa
topic1|2|7|36|5.5,3.5,1.3,0.2,Iris-setosa
topic1|2|8|40|5.0,3.5,1.3,0.3,Iris-setosa
topic1|2|9|41|4.5,2.3,1.3,0.3,Iris-setosa
```

### 2. Open Pycharm Editor 
  
Create these files;
  -.gitignore
  - requirements.txt
  - admin_client.py


### 3. Create a Topic

```
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
import time

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092', 'localhost:9292'],
                                client_id='dataops_client')

# List topics
print("Created topics", admin_client.list_topics())

# Create a topic
try:
    homework7 = NewTopic(name='homework7', num_partitions=2, replication_factor=2)

    admin_client.create_topics(new_topics=[homework7])
except:
    print("Topics are already exist.")


# List topics
time.sleep(2)
print("After create topics", admin_client.list_topics())
```


### 4. Create a Consumer.py

```
from message_parser import MessageParser
from kafka import KafkaConsumer
import re


consumer = KafkaConsumer('homework7',
                         group_id='group1',

                         auto_offset_reset='earliest',

                         enable_auto_commit=False,

                         consumer_timeout_ms=10000,

                         bootstrap_servers=['localhost:9092'])

setosa_file_obj = open("/home/train/PycharmProjects/week_2_homework_7/tmp/kafka_out/setosa_out.txt", "a")
versicolorfile_obj = open("/home/train/PycharmProjects/week_2_homework_7/tmp/kafka_out/versicolor_out.txt", "a")
virginicafile_obj = open("/home/train/PycharmProjects/week_2_homework_7/tmp/kafka_out/virginica_out.txt", "a")
other_obj = open("/home/train/PycharmProjects/week_2_homework_7/tmp/kafka_out/other_out.txt", "a")
mp = MessageParser()

for message in consumer:

    print("topic: %s, partition: %d, offset: %d, key: %s value: %s" % (message.topic,
                                                 message.partition,
                                                 message.offset,
                                                 message.key.decode('utf-8'),
                                                 message.value.decode('utf-8')))

    species = mp.message_splitter(message.value.decode('utf-8'))
    print("Species: {} ".format(species))

    if species == "setosa":
        setosa_file_obj.write(
            message.topic + "|" + str(message.partition) + "|" + str(message.offset) + "|" + message.key.decode(
                'utf-8') + "|" + message.value.decode('utf-8') + "\n")

    elif species == "versicolor":
        versicolorfile_obj.write(
            message.topic + "|" + str(message.partition) + "|" + str(message.offset) + "|" + message.key.decode(
                'utf-8') + "|" + message.value.decode('utf-8') + "\n")

    elif species == "virginica":
        virginicafile_obj.write(
            message.topic + "|" + str(message.partition) + "|" + str(message.offset) + "|" + message.key.decode(
                'utf-8') + "|" + message.value.decode('utf-8') + "\n")
    else:
        other_obj.write(
            message.topic + "|" + str(message.partition) + "|" + str(message.offset) + "|" + message.key.decode(
                'utf-8') + "|" + message.value.decode('utf-8') + "\n")


setosa_file_obj.close()
versicolorfile_obj.close()
virginicafile_obj.close()
other_obj.close()

```


### 5. Create a MessageParser.py

```
import re
class MessageParser:

    def message_splitter(self, msg):
        species = re.split(",", msg)[-1]
        switcher = {
            'Iris-setosa': "setosa",
            'Iris-versicolor': "versicolor",
            'Iris-virginica': "virginica",
            None: "other"
        }
        return switcher.get(species)

```

### 6. Open Terminal

```
[train@trainvm ~]$ cd data-generator/
```

```
[train@trainvm ~]$ source datagen/bin/activate
```

```
 (datagen) [train@trainvm data-generator]$ python dataframe_to_kafka.py -t homework7
