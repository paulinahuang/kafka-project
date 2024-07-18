import csv
import json
from kafka import KafkaProducer

employee_topic_name = "employee_salaries"

class CaphcaProducer:
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        self.producer = KafkaProducer(bootstrap_servers=f"{self.host}:{self.port}")

    def produce_messages(self, csv_file):
        with open(csv_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Filter by department ECC, CIT, EMS and hire date after 2010
                if row['Department'] in ['ECC', 'CIT', 'EMS'] and int(row['Initial Hire Date'].split('-')[-1]) >= 2010:
                    # Round salary to lower number
                    salary = int(float(row['Salary']))
                    message = {
                        "department": row['Department'],
                        "department_division": row['Department-Division'],
                        "position_title": row['Position Title'],
                        "hire_date": row['Initial Hire Date'],
                        "salary": salary
                    }
                    # Send message to Kafka topic
                    self.producer.send(employee_topic_name, value=json.dumps(message).encode('utf-8'))

if __name__ == '__main__':
    producer = CaphcaProducer()
    producer.produce_messages('Employee_Salaries.csv')