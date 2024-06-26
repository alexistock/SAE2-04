import paho.mqtt.client as mqtt
import mysql.connector
import time
from datetime import datetime

# Configuration du broker et du topic
BROKER = "test.mosquitto.org"
TOPIC = "IUT/Colmar2024/SAE2.04/Maison1"

# Configuration de la base de données MySQL
DB_HOST = "localhost"
DB_USER = "toto"
DB_PASSWORD = "toto"
DB_NAME = "my_db"

# Connexion à la base de données
db_connection = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)
db_cursor = db_connection.cursor()

# Callback pour la connexion
def on_connect(client, userdata, flags, rc):
    print(f"Connecté avec le code {rc}")
    client.subscribe(TOPIC)
    print(f"Souscrit au topic: {TOPIC}")

# Callback pour la réception des messages
def on_message(client, userdata, msg):
    message = msg.payload.decode()
    print(f"Message reçu sur {msg.topic}: {message}")
    data = extract_data(message)
    insert_data_to_db(data)

# Fonction pour extraire les données du message
def extract_data(message):
    data = {}
    items = message.split(',')
    for item in items:
        if '=' in item:
            key, value = item.split('=', 1)
            data[key.strip()] = value.strip()
    return data

# Fonction pour insérer les données dans la base de données
def insert_data_to_db(data):
    # Récupérer l'ID du capteur basé sur le Nom du capteur
    db_cursor.execute("SELECT ID FROM Capteurs WHERE Nom = %s", (data['Id'],))
    result = db_cursor.fetchone()
    if result:
        capteur_id = result[0]
    else:
        # Insérer le capteur dans la table Capteurs si non existant
        db_cursor.execute("INSERT INTO Capteurs (Nom, Piece, Emplacement) VALUES (%s, %s, %s)", (data['Id'], data['piece'], ""))
        db_connection.commit()
        capteur_id = db_cursor.lastrowid

    # Conversion de la date et de l'heure au format MySQL
    datetime_str = f"{data['date']} {data['time']}"
    datetime_obj = datetime.strptime(datetime_str, "%d/%m/%Y %H:%M:%S")
    mysql_datetime = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    # Insérer les données dans la table Donnees
    query = "INSERT INTO Donnees (CapteurID, Timestamp, Valeur) VALUES (%s, %s, %s)"
    values = (capteur_id, mysql_datetime, float(data['temp']))
    db_cursor.execute(query, values)
    db_connection.commit()

# Configuration du client MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connexion au broker
client.connect(BROKER, 1883, 60)

# Boucle infinie pour maintenir la connexion et écouter les messages
try:
    client.loop_start()
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    client.loop_stop()
    db_cursor.close()
    db_connection.close()
    print("Arrêt du script")