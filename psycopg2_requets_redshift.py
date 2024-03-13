import psycopg2

"""
Etape 1 : créer un cluster Redshift 
"""

# Paramètres de connexion à votre cluster Redshift
host = "votre-cluster.redshift.amazonaws.com"
port = "5439"  # Le port par défaut de Redshift
dbname = "votre_base_de_donnees"
user = "votre_utilisateur"
password = "votre_mot_de_passe"

# Chaîne de connexion
conn_string = f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'"

# Connexion à Redshift
try:
    conn = psycopg2.connect(conn_string)
    print("Connecté à Redshift")
    
    # Création d'un curseur pour exécuter des requêtes
    cursor = conn.cursor()
    
    # Exemple de requête SQL
    query = "SELECT version();"
    cursor.execute(query)
    
    # Récupérer les résultats
    results = cursor.fetchone()
    print(results)
    
    # Fermeture de la connexion
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Erreur lors de la connexion à Redshift : {e}")
