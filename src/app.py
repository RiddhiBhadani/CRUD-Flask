import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

from dotenv import load_dotenv
load_dotenv()

DB_PASSWORD = os.environ.get('DB_PASSWORD')

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        dbname="sampledb",
        user="riddhi_psql",
        password=DB_PASSWORD,
        host="localhost"
    )
    return conn

# Create (Insert)
@app.route('/register', methods=['POST'])
def create_registration():
    data = request.get_json()
    name = data.get('name')
    email = data.get('email')
    password = data.get('password')
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users_schema.registration (name, email, password)
        VALUES (%s, %s, %s) RETURNING id;
    """, (name, email, password))
    reg_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"id": reg_id, "message": "Registration successful"}), 201

# Read (Retrieve all)
@app.route('/registrations', methods=['GET'])
def get_registrations():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users_schema.registration;")
    registrations = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(registrations)

# Update
@app.route('/register/<int:id>', methods=['PUT'])
def update_registration(id):
    data = request.get_json()
    name = data.get('name')
    email = data.get('email')
    password = data.get('password')
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users_schema.registration
        SET name=%s, email=%s, password=%s
        WHERE id=%s;
    """, (name, email, password, id))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Registration updated successfully"})

# Delete
@app.route('/register/<int:id>', methods=['DELETE'])
def delete_registration(id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM users_schema.registration WHERE id=%s;", (id,))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Registration deleted successfully"})

if __name__ == '__main__':
    app.run(debug=True)
