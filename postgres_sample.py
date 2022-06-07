## precisa instalar o postgres no os
## brew install postgres
## pip install psycopg2 flask flask-cors
## export FLASK_APP=postgres_sample && flask run

import os
import psycopg2
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def get_db_connection():
    #conn = psycopg2.connect(host='localhost',
    #                        database='flask_db',
    #                        user=os.environ['DB_USERNAME'],
    #                        password=os.environ['DB_PASSWORD'])
    conn = psycopg2.connect(host='hh-pgsql-public.ebi.ac.uk',
                            database='pfmegrnargs',
                            user='reader',
                            password='NWDMCE5xdipIjRrp')
    return conn

def get_results():
    query = "SELECT * FROM rnc_database"
    with get_db_connection() as conn:
        with  conn.cursor() as cursor:
            cursor.execute(query) # .fetchall() ou .fetmany(100)
            dt = [row[3:5] for row in cursor]
            return dt

@app.route('/')
def index():
    return jsonify(get_results())
    