from kafka import KafkaProducer
import json

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

producer.send("bank_branch", {'atm_id': 1, 'trans_id': 100})
producer.send("bank_branch", {'atm_id': 2, 'trans_id': 101})
producer.flush()
producer.close()
