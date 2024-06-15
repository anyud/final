import os


def execute_sql_scripts(cursor, directory):
    for filename in os.listdir(directory):
        if filename.endswith(".sql"):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as file:
                sql_script = file.read()
                cursor.execute(sql_script)
                print(f"Executed {filename} successfully.")
