from dotenv import load_dotenv
load_dotenv()
import os
uri=os.getenv("DB_Connection_url")
from langchain.chains import create_sql_query_chain
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
class db_operations:
    """
        uuid
        student_id:
        first_name:
        last_name:
        date_of_birth:
        subject:
        marks:
        created_at:
        modified_at: Defined a function and trigger operation
    """
    def __init__(self,user_query):
        llm=ChatOpenAI(model="gpt-4o-mini")
        self.db = SQLDatabase.from_uri(os.getenv("DB_Connection_url"))
        chain = create_sql_query_chain(llm, self.db)
        self.response = chain.invoke({"question": user_query})
        print(self.response)
    def rectify_read_query(self,response):
        select_index = response.find("SELECT")
        semicolon_index = response.find(";", select_index)
        if select_index != -1 and semicolon_index != -1:
            return response[select_index:semicolon_index + 1]
        else:
            return None
    def rectify_update_query(self,response):
        select_index = response.find("UPDATE")
        semicolon_index = response.find(";", select_index)
        if select_index != -1 and semicolon_index != -1:
            return response[select_index:semicolon_index + 1]
        else:
            return None
    def rectify_insert_query(self,response):
        select_index = response.find("INSERT")
        semicolon_index = response.find(";", select_index)
        if select_index != -1 and semicolon_index != -1:
            return response[select_index:semicolon_index + 1]
        else:
            return None
    def rectify_delete_query(self,response):
        select_index = response.find("DELETE")
        semicolon_index = response.find(";", select_index)
        if select_index != -1 and semicolon_index != -1:
            return response[select_index:semicolon_index + 1]
        else:
            return None
        
    def perform_db_operation(self):
        if "SELECT" in self.response:
            read_query=self.rectify_read_query(self.response)
            self.db.run(read_query)
            return "Read operation performed please check the Table"
        elif "INSERT" in self.response:
            insert_query=self.rectify_insert_query(self.response)
            self.db.run(insert_query)
            return "Insert operation performed Please check the Table"
        elif "DELETE" in self.response:
            delete_query=self.rectify_delete_query(self.response)
            self.db.run(delete_query)
            return "Delete operation performed Please check the Table"
        elif "UPDATE" in self.response:
            update_query=self.rectify_update_query(self.response)
            self.db.run(update_query)
            return "Update operation performed Please check the Table"
        else:
            return "There is any other operation found apart from read,delete,update or insert so not executing query"
    @staticmethod
    def get_latest_record():
        ret_query="SELECT student_id, first_name, last_name, subject,marks FROM students_records ORDER BY edited_at DESC LIMIT 1;"
        db = SQLDatabase.from_uri(os.getenv("DB_Connection_url"))
        return db.run(ret_query)