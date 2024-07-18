import psycopg2

class DataIngestor:
    def __init__(self):
        # Initialize database connection
        self.conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="huangbaojia"
            # password="your_password"  # 如果有密码的话
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def update_department_salary(self):
        try:
            # Clear the department_employee_salary table
            self.cur.execute("DELETE FROM department_employee_salary")

            # Calculate total salary by department
            self.cur.execute("""
                INSERT INTO department_employee_salary (department, total_salary)
                SELECT department, SUM(salary)
                FROM department_employee
                GROUP BY department
            """)

        except psycopg2.Error as e:
            print(f"Error updating department salary: {e}")

    def close_connection(self):
        self.cur.close()
        self.conn.close()

if __name__ == "__main__":
    ingestor = DataIngestor()
    ingestor.update_department_salary()
    ingestor.close_connection()
