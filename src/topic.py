import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from common import EVIDENCE, save_json

TOPIC='viajes_taxi'
BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP','localhost:9092')

def ensure_topic():
    admin=KafkaAdminClient(bootstrap_servers=BOOTSTRAP,client_id='unad-admin')
    try:
        admin.create_topics([NewTopic(TOPIC,num_partitions=3,replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    info=admin.describe_topics([TOPIC])[0]
    assert len(info['partitions'])==3, info
    assert all(len(p['replicas'])==1 for p in info['partitions']),info
    save_json(EVIDENCE/'topic.json',info)
    admin.close()
    return info

if __name__=='__main__': print(ensure_topic())
