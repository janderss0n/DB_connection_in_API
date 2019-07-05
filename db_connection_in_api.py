import json
import sqlalchemy
import pandas as pd
from flask import Flask, request
from datetime import datetime

app = Flask(__name__)

def fetch_db_secrets():
    return {
        "databasedialect": "dialect",
        "host": "host",
        "port": "port_numbers_here",
        "user": "postgres",
        "password": "password",
        "database": "my_database",
        "table": "table_name"
    }


class OnlyOne:
    class __OnlyOne:
        def __init__(self, secrets):
            self.engine = sqlalchemy.create_engine(
                '{}://{}:{}@{}:{}/{}'.format(secrets['databasedialect'],secrets['user'],secrets['password'],secrets['host'],secrets['port'],secrets['database'])
            )
        def __str__(self):
            return self.engine

    instance = None
    def __init__(self):
        self.secrets = fetch_db_secrets()
        if not OnlyOne.instance:
            OnlyOne.instance = OnlyOne.__OnlyOne(self.secrets)
    def __getattr__(self, name):
        return getattr(self.instance, name)


@app.route('/saveToDB', methods=['POST'])
def write_df_to_db():
    written_df_to_db = False
    json_ = request.json
    df = pd.DataFrame(json_)

    inst = OnlyOne()
    try:
        con = inst.engine.connect()
        try:
            df.to_sql(inst.secrets['table'], con, index=False, if_exists='append')
            written_df_to_db = True
        except:
            print('Faild to write table to DB')
        finally:
            con.close()
    except:
        print('Could not connect to DB')

    return ('DF saved in DB' if written_df_to_db else 'Could not save df to DB')


if __name__=='__main__':
    app.run(port=1235, debug=True)
