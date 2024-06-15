import os


def execute_sql_scripts(cursor, script_files):
    for file_path in script_files:
        with open(file_path, 'r') as file:
            sql_script = file.read()
            cursor.execute(sql_script)
            print(f"Executed {file_path} successfully.")



