# from kafka import KafkaProducer
# import json
# import random
# import time

# topic = "randomuser_data"
# kafka_config = {
#     "bootstrap_servers": "localhost:9092",  # Change this to your Kafka server address
# }

# producer = KafkaProducer(bootstrap_servers=kafka_config["bootstrap_servers"])

# while True:
#     # Generate random user data
#     time.sleep(7)
#     user_data = {
#         "results": [
#             {
#                 "gender": random.choice(["male", "female"]),
#                 "name": {
#                     "title": "Mr" if random.random() < 0.5 else "Ms",
#                     "first": "John" if random.random() < 0.5 else "Jane",
#                     "last": "Doe" if random.random() < 0.5 else "Smith",
#                 },
#                 "location": {
#                     "street": {
#                         "number": random.randint(1000, 9999),
#                         "name": "Random St",
#                     },
#                     "city": "Random City",
#                     "state": "Random State",
#                     "country": "Random Country",
#                     "postcode": str(random.randint(10000, 99999)) + " " + random.choice(["ABC", "DEF"]),
#                 },
#                 "email": "user@example.com",
#             }
#         ],
#         "info": {
#             "seed": "randomseed",
#             "results": 1,
#             "page": 1,
#             "version": "1.4",
#         }
#     }

#     # Serialize user data to JSON
#     user_data_json = json.dumps(user_data)

#     # Print the generated data
#     print("Generated user data:")
#     print(user_data_json)

#     # Send the data to Kafka
#     producer.send(topic, value=user_data_json.encode("utf-8"))
#     producer.flush()

import time  
from kafka import KafkaProducer 
from kafka.errors import NoBrokersAvailable
import requests
from requests.exceptions import SSLError
import logging
import sys
import json

logging.basicConfig(level=logging.ERROR)

# Configuration du producteur Kafka
server = 'localhost:9092'
topic_name = 'user_profiles'

# Création d'une instance de producteur
try:
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
except NoBrokersAvailable as ne:
    logging.error('No brokers available')
    sys.exit()

# Remplacez cette URL par l'API que vous souhaitez consommer
api_url = 'https://randomuser.me/api/'

error_count = 0
users = []
while True:
    try:
        # Récupérez des données depuis l'API
        response = requests.get(api_url)
        data = response.json()['results'][0]
    
        # Publiez les données dans un topic Kafka
        producer.send(topic_name, value=data)
        producer.flush()
        
        print(f"User number {len(users)+1}")
        users.append(data)

    except Exception as e:
        print("Error: ", e)
        for i in range(10):
            print(f"Waiting {i+1}", end="\r")
            time.sleep(1)
        if error_count > 5:
            continue
        error_count += 1

# Fermez le producteur Kafka lorsque vous avez terminé
producer.close()
