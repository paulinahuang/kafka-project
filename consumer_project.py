import json
import psycopg2
from kafka import KafkaConsumer

employee_topic_name = "employee_salaries"

class CaphcaConsumer:
    def __init__(self, host="localhost", port="29092", group_id="employee_consumer"):
        self.consumer = KafkaConsumer(
            employee_topic_name,
            bootstrap_servers=f"{host}:{port}",
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def consume_messages(self):
        try:
            for msg in self.consumer:
                employee_data = msg.value
                # Insert into PostgreSQL and update total salary by department
                persist_employee(employee_data)
        finally:
            self.consumer.close()

def persist_employee(employee_data):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="huangbaojia"
            #password=""
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Insert data into Department_Employee table
        insert_query = """
            INSERT INTO Department_Employee (department, department_division, position_title, hire_date, salary)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            employee_data['department'],
            employee_data['department_division'],
            employee_data['position_title'],
            employee_data['hire_date'],
            employee_data['salary']
        ))

        # Update total salary in Department table
        update_query = """
            UPDATE public.Department
            SET total_salary = total_salary + %s
            WHERE department = %s
        """
        cur.execute(update_query, (
            employee_data['salary'],
            employee_data['department']
        ))

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    consumer = CaphcaConsumer()
    consumer.consume_messages()
