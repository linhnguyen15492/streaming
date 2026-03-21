from kafka import KafkaProducer
import json

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
trans_id = 102
while True:
    user_input = input("Do you want to add a transaction? (press 'n' to stop): ")
    if user_input.lower() == 'n':
        print("Stopping the transactions")
        break
    else:
        atm_choice = input("Which ATM you want to transact in? 1 or 2 ")
        if atm_choice == '1' or atm_choice == '2':
            producer.send("bank_branch", {'atm_id': int(atm_choice), 'trans_id': trans_id})
            producer.flush()
            trans_id = trans_id + 1
        else:
            print('Invalid ATM number')
            continue

producer.close()
