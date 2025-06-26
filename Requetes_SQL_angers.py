# Import des librairies nécessaires
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.pool import QueuePool
import logging
from datetime import datetime
from time import sleep
from sqlalchemy.exc import OperationalError
from MySQLdb import OperationalError as MySQLOperationalError
from Cles_API_et_BDD import DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_CHARSET

# Configuration des logs
logging.basicConfig(filename='Logs/Logs_RequeteSQL_general_angers.txt', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# Ajout de la configuration pour afficher les logs dans le terminal (CMD)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logging.getLogger().addHandler(stream_handler)

# Nom de la base de données
database = 'bitrix_angers'

# Création de l'engine avec un pool de connexions
try:
    engine = create_engine(
        f"mysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOSTNAME}/{database}?charset=utf8",
        connect_args={'sql_mode': 'ALLOW_INVALID_DATES', 'autocommit': True},
        poolclass=QueuePool,
        pool_size=15,  # Nombre de connexions à maintenir dans le pool
        max_overflow=15,  # Nombre maximal de connexions "hors pool"
        pool_timeout=60,  # Temps d'attente maximum en secondes pour une connexion du pool
        pool_recycle=1600  # Temps en secondes pour recycler une connexion
    )
    conn = engine.connect().execution_options(autocommit=True)
    logging.info("Connexion à la base de données avec pool de connexions réussie.")
except Exception as e:
    logging.error(f"Erreur de connexion à la base de données avec pool de connexions : {e}")
    raise

#------------------------------------------ Duplication des tables pour la sauvegarde ------------------------------------------
tables_a_sauvegarder = ["entity_contact", "crm_contact", "crm_contact_uf", "entity_deal", "crm_deal", "crm_deal_uf", "entity_lead", "crm_lead", "crm_lead_uf","entity_company","crm_company_uf","crm_company"]
for table in tables_a_sauvegarder:
    backup_table = f"{table}_transform"

    # Vérifiez si la table de sauvegarde existe déjà
    table_exists_query = text(f"SHOW TABLES LIKE '{backup_table}'")
    table_exists = conn.execute(table_exists_query).fetchone()

    if table_exists:
        # Supprimez la table de sauvegarde si elle existe déjà
        drop_table_query = text(f"DROP TABLE {backup_table}")
        conn.execute(drop_table_query)
        logging.info(f"Table de Transformation {backup_table} supprimée.")

    # Créez une copie de la table
    if table == "crm_deal_uf":
        copy_table_query = text(f"CREATE TABLE {backup_table} ENGINE=MyISAM AS SELECT * FROM {table}")
    else:
        copy_table_query = text(f"CREATE TABLE {backup_table} ENGINE=InnoDB ROW_FORMAT=COMPRESSED AS SELECT * FROM {table}")
    conn.execute(copy_table_query)
    logging.info(f"Table de Transformation {backup_table} créée avec l'engine approprié.")

    # Attendre avant de faire la suite
    sleep(5)


# Récupération des noms des tables
metadata = MetaData()
metadata.reflect(bind=engine)
table_names = metadata.tables.keys()


# Liste des tables à traiter
kanban_list_final = ['Contacts', 'Transactions', 'Prospects', 'Entreprise']
table_list_Entreprise_final = ['Entreprise']
table_list_Prospects_final = ['Prospects','Prospects_TC']
table_list_Contacts_final = ['Contacts','Contact_Entreprise','Tuteur','Tuteur_2','Responsable_légal_1','Responsable_légal_2','Répondant_Financier_1','Répondant_Financier_2','Contact_en_cas_durgence','Responsable_Comptable','Dirigeant','Interlocuteur_OPCO','Décideur_de_recrutement']
table_list_Transactions_final = ['Transactions','Transactions_Alternant','Transactions_Initial','Transactions_Entreprise','Transactions_FPC',"Transactions_Prise_en_charge_OPCO"]

# Établissement de la connexion
conn = engine.connect()


# Liste des tables et des colonnes pour créer des index
Colonne_a_indexer = [
    ("crm_contact_transform", "ID"),
    ("crm_contact_uf_transform", "CONTACT_ID"),
    ("entity_contact_transform", "entity_contact_ID"),
    ("crm_lead_transform", "ID"),
    ("crm_lead_uf_transform", "LEAD_ID"),
    ("entity_lead_transform", "entity_lead_ID"),
    ("crm_deal_transform", "ID"),
    ("crm_deal_uf_transform", "DEAL_ID"),
    ("entity_deal_transform", "entity_deal_ID"),
    ("entity_deal_transform", "sql_UF_CRM_1589207602"),
    ("entity_deal_transform", "sql_UF_CRM_1588240383"),
    ("entity_deal_transform", "sql_UF_CRM_1602495367"),
    ("entity_deal_transform", "sql_UF_CRM_1583301691"),
    ("entity_deal_transform", "sql_UF_CRM_1588147809"),
    ("entity_deal_transform", "sql_UF_CRM_1617110018"),
    ("entity_deal_transform", "sql_UF_CRM_1642057159"),
    ("entity_deal_transform", "sql_UF_CRM_1591262278"),
    ("entity_company_transform", "sql_UF_CRM_1589375441"),
    ("entity_company_transform", "sql_UF_CRM_1589375694"),
    ("entity_company_transform", "sql_UF_CRM_1589355778"),
    ("entity_company_transform", "sql_UF_CRM_1589355778"),
    ("entity_company_transform", "entity_company_ID"),
    ("crm_company_uf_transform", "COMPANY_ID"),
    ("crm_company_transform", "ID")
]

# Création des INDEX
for table, column in Colonne_a_indexer:
    # Vérifiez si l'index existe
    index_check_query = text(f"SHOW INDEX FROM {table} WHERE Column_name = '{column}'")
    index_exists = conn.execute(index_check_query).fetchone()

    if not index_exists:
        # Créer l'index s'il n'existe pas
        create_index_query = text(f"CREATE INDEX idx_{column}_on_{table} ON {table}({column})")
        conn.execute(create_index_query)
        logging.info(f"Index sur {column} de la table {table} créé.")
    else:
        logging.info(f"Index sur {column} de la table {table} existe déjà.")
    # Attend 1s avant de faire le prochain INDEX
    sleep(1)
sleep(5)



def Reconnexion():
    global conn
    # Vérification de la connexion à la base de données après l'application des modifications
    if not conn.closed:
        try:
            conn.close()
            sleep(2) # Attend 5s avant de faire la suite
            logging.info("Connexion à la base de données fermée après l'application des modifications.")
        except Exception as e:
            logging.error(f"Erreur lors de la fermeture de la connexion à la base de données : {e}")

    # Rétablissement de la connexion à la base de données si elle a été fermée
    try:
        conn = engine.connect().execution_options(autocommit=True)
        logging.info("Connexion à la base de données rétablie après l'application des modifications.")
    except Exception as e:
        logging.error(f"Erreur lors du rétablissement de la connexion à la base de données : {e}")



#------------------------------------------ Changement de kanban > Contacts--------------------------------------------------
Reconnexion()

if 'Contacts' in kanban_list_final:
    table_prioritaire_a_verifier_Contacts = ['crm_contact_transform']
    table_secondaire_a_verifier_Contacts = ['crm_contact_uf_transform', 'entity_contact_transform']
    doublon_Contacts = []

    query_table_prioritaire_doublon_Contacts = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_prioritaire_a_verifier_Contacts[0]}'")
    result_table_prioritaire_doublon_Contacts = conn.execute(query_table_prioritaire_doublon_Contacts)
    list_table_prioritaire_doublon_Contacts = [row[0] for row in result_table_prioritaire_doublon_Contacts if row[0] not in ['ID', 'ID_CONTACT']]
    logging.info(f"Liste des doublons dans la table prioritaire : {list_table_prioritaire_doublon_Contacts}")

    # Supprimer les colonnes de "list_table_prioritaire_doublon" dans les tables secondaires
    #Si certaines données manquent, cette partie peut etre mis en pause afin de ravoir l'entièreté des données sur la BDD
    for table in table_secondaire_a_verifier_Contacts:
        for col in list_table_prioritaire_doublon_Contacts:
            try:
                # Vérifier d'abord si la colonne existe
                if conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE '{col}'")).fetchone():
                    conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {col}"))
                    logging.info(f"Colonne {col} supprimée de la table {table}")
            except Exception as e:
                logging.error(f"Erreur lors de la suppression de la colonne {col} de la table {table}: {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Contacts
    if 'Contacts' in table_list_Contacts_final:
        # Vérifier si la table Contacts existe, si oui, la supprimer
        if 'Contacts' in table_names:
            conn.execute(text("DROP TABLE Contacts"))
            logging.info("Table Contacts supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Contacts avec le SELECT
        select_query_Contacts = text("""
            CREATE TABLE Contacts AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            ORDER BY
                ID ASC""")
        try:
            result_Contacts = conn.execute(select_query_Contacts)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Contacts exécutée avec succès.")

            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Contacts ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Contacts appliquée.")

        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Contacts : {e}")

    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Contact Entreprise
    if 'Contact_Entreprise' in table_list_Contacts_final:
        # Vérifier si la table Contacts Entreprise existe, si oui, la supprimer
        if 'Contact_Entreprise' in table_names:
            conn.execute(text("DROP TABLE Contact_Entreprise"))
            logging.info("Table Contact Entreprise supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Contacts Entreprise avec le SELECT
        select_query_Contact_Entreprise = text("""
            CREATE TABLE Contact_Entreprise AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            INNER JOIN
                entity_deal_transform ON crm_contact_transform.ID = entity_deal_transform.sql_UF_CRM_1589207602
            ORDER BY
                ID ASC;""")
        try:
            result_Contact_Entreprise = conn.execute(select_query_Contact_Entreprise)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Contact Entreprise exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Contact_Entreprise ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Contact Entreprise appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Contact Entreprise : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Contact en cas d'urgence
    if 'Contact_en_cas_durgence' in table_list_Contacts_final:
        # Vérifier si la table Contact en cas d'urgence existe, si oui, la supprimer
        if 'Contact_en_cas_durgence' in table_names:
            conn.execute(text("DROP TABLE Contact_en_cas_durgence"))
            logging.info("Table Contact_en_cas_durgence supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Contact en cas d'urgence avec le SELECT
        select_query_Contact_en_cas_durgence = text("""
            CREATE TABLE Contact_en_cas_durgence AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            WHERE
                crm_contact_uf_transform.UF_CRM_1643183509 IS NOT NULL
            ORDER BY
                ID ASC""")
        try:
            result_Contact_en_cas_durgence = conn.execute(select_query_Contact_en_cas_durgence)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Contact en cas d'urgence exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Contact_en_cas_durgence ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Contact en cas d'urgence appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Contact en cas d'urgence : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    #Création de la table Tuteur
    if 'Tuteur' in table_list_Contacts_final:
        # Vérifier si la table Tuteur existe, si oui, la supprimer
        if 'Tuteur' in table_names:
            conn.execute(text("DROP TABLE Tuteur"))
            logging.info("Table Tuteur supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Tuteur avec le SELECT
        select_query_Tuteur = text("""
            CREATE TABLE Tuteur AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            INNER JOIN
                entity_deal_transform ON crm_contact_transform.ID = entity_deal_transform.sql_UF_CRM_1588240383
            ORDER BY
                ID ASC;""")
        try:
            result_Tuteur = conn.execute(select_query_Tuteur)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Tuteur1 exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Tuteur ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Tuteur1 appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Tuteur1 : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Tuteur 2
    if 'Tuteur_2' in table_list_Contacts_final:
        # Vérifier si la table Tuteur2 existe, si oui, la supprimer
        if 'Tuteur2' in table_names:
            conn.execute(text("DROP TABLE Tuteur2"))
            logging.info("Table Tuteur2 supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Tuteur2 avec le SELECT
        select_query_Tuteur = text("""
            CREATE TABLE Tuteur2 AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            INNER JOIN
                entity_deal_transform ON crm_contact_transform.ID = entity_deal_transform.sql_UF_CRM_1602495367
            ORDER BY
                ID ASC;""")
        try:
            result_Tuteur = conn.execute(select_query_Tuteur)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Tuteur2 exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Tuteur2 ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Tuteur2 appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Tuteur2 : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Resposanble légal 1
    if 'Responsable_légal_1' in table_list_Contacts_final:
        # Vérifier si la table Responsable légal 1 existe, si oui, la supprimer
        if 'Responsable_légal_1' in table_names:
            conn.execute(text("DROP TABLE Responsable_légal_1"))
            logging.info("Table Responsable légal 1 supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Responsable légal 1 avec le SELECT
        select_query_Responsable_légal_1 = text("""
            CREATE TABLE Responsable_légal_1 AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            WHERE
                crm_contact_uf_transform.UF_CRM_1606994316 IS NOT NULL
            ORDER BY
                ID ASC""")
        try:
            result_Responsable_légal_1 = conn.execute(select_query_Responsable_légal_1)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Responsable légal 1 exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Responsable_légal_1 ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Responsable légal 1 appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Responsable légal 1 : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Resposanble légal 2
    if 'Responsable_légal_2' in table_list_Contacts_final:
        # Vérifier si la table Responsable légal 2 existe, si oui, la supprimer
        if 'Responsable_légal_2' in table_names:
            conn.execute(text("DROP TABLE Responsable_légal_2"))
            logging.info("Table Responsable légal 2 supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Responsable légal 2 avec le SELECT
        select_query_Responsable_légal_2 = text("""
            CREATE TABLE Responsable_légal_2 AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            WHERE
                crm_contact_uf_transform.UF_CRM_1606994331 IS NOT NULL
            ORDER BY
                ID ASC""")
        try:
            result_Responsable_légal_2 = conn.execute(select_query_Responsable_légal_2)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Responsable légal 2 exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Responsable_légal_2 ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Responsable légal 2 appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Responsable légal 2 : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Répondant Financier 1
    if 'Répondant_Financier_1' in table_list_Contacts_final:
        # Vérifier si la table Répondant Financier 1 existe, si oui, la supprimer
        if 'Répondant_Financier_1' in table_names:
            conn.execute(text("DROP TABLE Répondant_Financier_1"))
            logging.info("Table Répondant Financier 1 supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Répondant Financier 1 avec le SELECT
        select_query_Répondant_Financier_1 = text("""
            CREATE TABLE Répondant_Financier_1 AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            WHERE
                UF_CRM_1604401780 IS NOT NULL
            ORDER BY
                ID ASC""")
        try:
            result_Répondant_Financier_1 = conn.execute(select_query_Répondant_Financier_1)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Répondant Financier 1 exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Répondant_Financier_1 ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Répondant Financier 1 appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Répondant Financier 1 : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Répondant Financier 2
    if 'Répondant_Financier_2' in table_list_Contacts_final:
        # Vérifier si la table Répondant Financier 2 existe, si oui, la supprimer
        if 'Répondant_Financier_2' in table_names:
            conn.execute(text("DROP TABLE Répondant_Financier_2"))
            logging.info("Table Répondant Financier 2 supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Répondant Financier 2 avec le SELECT
        select_query_Répondant_Financier_2 = text("""
            CREATE TABLE Répondant_Financier_2 AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            WHERE
                UF_CRM_1604401811 IS NOT NULL
            ORDER BY
                ID ASC""")
        try:
            result_Répondant_Financier_2 = conn.execute(select_query_Répondant_Financier_2)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Répondant Financier 2 exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Répondant_Financier_2 ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Répondant Financier 2 appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Répondant Financier 2 : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Responsable Comptable
    if 'Responsable_Comptable' in table_list_Contacts_final:
        # Vérifier si la table Responsable Comptable existe, si oui, la supprimer
        if 'Responsable_Comptable' in table_names:
            conn.execute(text("DROP TABLE Responsable_Comptable"))
            logging.info("Table Responsable_Comptable supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Responsable Comptable avec le SELECT
        select_query_Responsable_Comptable = text("""
            CREATE TABLE Responsable_Comptable AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            INNER JOIN
                entity_deal_transform ON crm_contact_transform.ID = entity_deal_transform.sql_UF_CRM_1591262278
            ORDER BY
                crm_contact_uf_transform.CONTACT_ID ASC;""")
        try:
            result_Responsable_Comptable = conn.execute(select_query_Responsable_Comptable)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Responsable Comptable exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Responsable_Comptable ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Responsable Comptable appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Responsable Comptable : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Dirigeant
    if 'Dirigeant' in table_list_Contacts_final:
        # Vérifier si la table Dirigeant existe, si oui, la supprimer
        if 'Dirigeant' in table_names:
            conn.execute(text("DROP TABLE Dirigeant"))
            logging.info("Table Dirigeant supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Dirigeant avec le SELECT
        select_query_Dirigeant = text("""
            CREATE TABLE Dirigeant AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            INNER JOIN
                entity_company_transform ON crm_contact_transform.ID = entity_company_transform.sql_UF_CRM_1589375441
            ORDER BY
                crm_contact_uf_transform.CONTACT_ID ASC;""")
        try:
            result_Dirigeant = conn.execute(select_query_Dirigeant)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Dirigeant exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Dirigeant ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Dirigeant appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Dirigeant : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Interlocuteur OPCO
    if 'Interlocuteur_OPCO' in table_list_Contacts_final:
        # Vérifier si la Interlocuteur OPCO existe, si oui, la supprimer
        if 'Interlocuteur_OPCO' in table_names:
            conn.execute(text("DROP TABLE Interlocuteur_OPCO"))
            logging.info("Table Interlocuteur OPCO supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Interlocuteur OPCO avec le SELECT
        select_query_Interlocuteur_OPCO = text("""
            CREATE TABLE Interlocuteur_OPCO AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            INNER JOIN
                entity_company_transform ON crm_contact_transform.ID = entity_company_transform.sql_UF_CRM_1589375694
            ORDER BY
                crm_contact_uf_transform.CONTACT_ID ASC;""")
        try:
            result_Interlocuteur_OPCO = conn.execute(select_query_Interlocuteur_OPCO)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Interlocuteur OPCO exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Interlocuteur_OPCO ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Interlocuteur OPCO appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Interlocuteur OPCO : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Décideur de recrutement
    if 'Décideur_de_recrutement' in table_list_Contacts_final:
        # Vérifier si la Décideur de recrutement existe, si oui, la supprimer
        if 'Décideur_de_recrutement' in table_names:
            conn.execute(text("DROP TABLE Décideur_de_recrutement"))
            logging.info("Table Décideur de recrutement supprimée avec succès.")
            # Attend 5s avant de faire la suite
            sleep(5)


        #Insertion des données dans Décideur de recrutement avec le SELECT
        select_query_Décideur_de_recrutement = text("""
            CREATE TABLE Décideur_de_recrutement AS
            SELECT
                crm_contact_transform.*,
                crm_contact_uf_transform.*,
                entity_contact_transform.*
            FROM
                crm_contact_transform
            LEFT JOIN
                crm_contact_uf_transform ON crm_contact_transform.ID = crm_contact_uf_transform.CONTACT_ID
            LEFT JOIN
                entity_contact_transform ON crm_contact_transform.ID = entity_contact_transform.entity_contact_ID
            INNER JOIN
                entity_company_transform ON crm_contact_transform.ID = entity_company_transform.sql_UF_CRM_1589355778
            ORDER BY
                crm_contact_uf_transform.CONTACT_ID ASC;""")
        try:
            result_Décideur_de_recrutement = conn.execute(select_query_Décideur_de_recrutement)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Décideur de recrutement exécutée avec succès.")
            # Modification de l'engine et du format de rangée
            alter_engine_query = text("ALTER TABLE Décideur_de_recrutement ENGINE=InnoDB ROW_FORMAT=COMPRESSED")
            conn.execute(alter_engine_query)
            logging.info("Modification de l'engine et du format de rangée pour Décideur de recrutement appliquée.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Décideur de recrutement : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)
    logging.info("Le scrypt sur les différentes tables Contacts est terminé")


#------------------------------------------ Changement de kanban > Transactions --------------------------------------------------
Reconnexion()

if 'Transactions' in kanban_list_final:
    table_prioritaire_a_verifier_Transactions = ['crm_deal_transform']
    table_secondaire_a_verifier_Transactions = ['crm_deal_uf_transform', 'entity_deal_transform']
    doublon_Transactions = []

    query_table_prioritaire_doublon_Transactions = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_prioritaire_a_verifier_Transactions[0]}'")
    result_table_prioritaire_doublon_Transactions = conn.execute(query_table_prioritaire_doublon_Transactions)
    list_table_prioritaire_doublon_Transactions = [row[0] for row in result_table_prioritaire_doublon_Transactions if row[0] not in ['ID', 'DEAL_ID']]
    logging.info(f"Liste des doublons dans la table prioritaire : {list_table_prioritaire_doublon_Transactions}")

    # Supprimer les colonnes de "list_table_prioritaire_doublon" dans les tables secondaires
    #Si certaines données manquent, cette partie peut etre mis en pause afin de ravoir l'entièreté des données sur la BDD
    for table in table_secondaire_a_verifier_Transactions:
        for col in list_table_prioritaire_doublon_Transactions:
            try:
                # Vérifier d'abord si la colonne existe
                if conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE '{col}'")).fetchone():
                    conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {col}"))
                    logging.info(f"Colonne {col} supprimée de la table {table}")
            except Exception as e:
                logging.error(f"Erreur lors de la suppression de la colonne {col} de la table {table}: {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Transaction
    if 'Transactions' in table_list_Transactions_final:
        # Vérifier si la table Transactions existe, si oui, la supprimer
        if 'Transactions' in table_names:
            conn.execute(text("DROP TABLE Transactions"))
            logging.info("Table Transactions supprimée avec succès.")
            sleep(5)

        #Insertion des données dans Transactions avec le SELECT
        select_query_Transactions = text("""
            CREATE TABLE Transactions
            ENGINE=MyISAM
            ROW_FORMAT=COMPRESSED
            KEY_BLOCK_SIZE=16
            AS
            SELECT
                crm_deal_transform.*,
                crm_deal_uf_transform.*,
                entity_deal_transform.*
            FROM
                crm_deal_transform
            LEFT JOIN
                crm_deal_uf_transform ON crm_deal_transform.ID = crm_deal_uf_transform.DEAL_ID
            LEFT JOIN
                entity_deal_transform ON crm_deal_transform.ID = entity_deal_transform.entity_deal_ID
            ORDER BY
                crm_deal_transform.ID ASC;""")

        try:
            result_Transactions = conn.execute(select_query_Transactions)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Transactions exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Transactions : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Transaction ALternant
    if 'Transactions_Alternant' in table_list_Transactions_final:
        # Vérifier si la table Transactions existe, si oui, la supprimer
        if 'Transactions_Alternant' in table_names:
            conn.execute(text("DROP TABLE Transactions_Alternant"))
            logging.info("Table Transactions Alternant supprimée avec succès.")


        #Insertion des données dans Transactions Alternant avec le SELECT
        #Les crm_deal_transform.CATEGORY_ID = 8 = Alternant
        select_query_Transactions_Alternant = text("""
            CREATE TABLE Transactions_Alternant
            ENGINE=MyISAM
            ROW_FORMAT=COMPRESSED
            KEY_BLOCK_SIZE=16
            AS
            SELECT
                crm_deal_transform.*,
                crm_deal_uf_transform.*,
                entity_deal_transform.*
            FROM
                crm_deal_transform
            LEFT JOIN
                crm_deal_uf_transform ON crm_deal_transform.ID = crm_deal_uf_transform.DEAL_ID
            LEFT JOIN
                entity_deal_transform ON crm_deal_transform.ID = entity_deal_transform.entity_deal_ID
            WHERE
                crm_deal_transform.CATEGORY_ID = 8
            ORDER BY
                crm_deal_transform.ID ASC;
            """)

        try:
            result_Transactions_Alternant = conn.execute(select_query_Transactions_Alternant)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Transactions Alternant exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Transactions Alternant : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Transaction Initial
    if 'Transactions_Initial' in table_list_Transactions_final:
        # Vérifier si la table Transactions existe, si oui, la supprimer
        if 'Transactions_Initial' in table_names:
            conn.execute(text("DROP TABLE Transactions_Initial"))
            logging.info("Table Transactions Initial supprimée avec succès.")


        #Insertion des données dans Transactions Alternant avec le SELECT
        #Les crm_deal_transform.CATEGORY_ID = 4 = Initial
        select_query_Transactions_Initial = text("""
            CREATE TABLE Transactions_Initial
            ENGINE=MyISAM
            ROW_FORMAT=COMPRESSED
            KEY_BLOCK_SIZE=16
            AS
            SELECT
                crm_deal_transform.*,
                crm_deal_uf_transform.*,
                entity_deal_transform.*
            FROM
                crm_deal_transform
            LEFT JOIN
                crm_deal_uf_transform ON crm_deal_transform.ID = crm_deal_uf_transform.DEAL_ID
            LEFT JOIN
                entity_deal_transform ON crm_deal_transform.ID = entity_deal_transform.entity_deal_ID
            WHERE
                crm_deal_transform.CATEGORY_ID = 4
            ORDER BY
                crm_deal_transform.ID ASC;""")
        try:
            result_Transactions_Initial = conn.execute(select_query_Transactions_Initial)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Transactions Initial exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Transactions Initial : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Transaction Entreprise
    if 'Transactions_Entreprise' in table_list_Transactions_final:
        # Vérifier si la table Transactions existe, si oui, la supprimer
        if 'Transactions_Entreprise' in table_names:
            conn.execute(text("DROP TABLE Transactions_Entreprise"))

            logging.info("Table Transactions Entreprise supprimée avec succès.")


        # Insertion des données dans Transactions Entreprise avec le SELECT
        #Les crm_deal_transform.CATEGORY_ID = 10 = Entreprise
        select_query_Transactions_Entreprise = text("""
            CREATE TABLE Transactions_Entreprise
            ENGINE=MyISAM
            ROW_FORMAT=COMPRESSED
            KEY_BLOCK_SIZE=16
            AS
            SELECT
                crm_deal_transform.*,
                crm_deal_uf_transform.*,
                entity_deal_transform.*
            FROM
                crm_deal_transform
            LEFT JOIN
                crm_deal_uf_transform ON crm_deal_transform.ID = crm_deal_uf_transform.DEAL_ID
            LEFT JOIN
                entity_deal_transform ON crm_deal_transform.ID = entity_deal_transform.entity_deal_ID
            WHERE
                crm_deal_transform.CATEGORY_ID = 10
            ORDER BY
                crm_deal_transform.ID ASC;""")
        try:
            result_Transactions_Entreprise = conn.execute(select_query_Transactions_Entreprise)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Transactions Entreprise exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Transactions Entreprise : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Transaction FPC
    if 'Transactions_FPC' in table_list_Transactions_final:
        # Vérifier si la table Transactions existe, si oui, la supprimer
        if 'Transactions_FPC' in table_names:
            conn.execute(text("DROP TABLE Transactions_FPC"))
            logging.info("Table Transactions FPC supprimée avec succès.")


        #Insertion des données dans Transactions FPC avec le SELECT
        #Les crm_deal_transform.CATEGORY_ID = 42 = Transaction FPC
        select_query_Transactions_FPC = text("""
            CREATE TABLE Transactions_FPC
            ENGINE=MyISAM
            ROW_FORMAT=COMPRESSED
            KEY_BLOCK_SIZE=16
            AS
            SELECT
                crm_deal_transform.*,
                crm_deal_uf_transform.*,
                entity_deal_transform.*
            FROM
                crm_deal_transform
            LEFT JOIN
                crm_deal_uf_transform ON crm_deal_transform.ID = crm_deal_uf_transform.DEAL_ID
            LEFT JOIN
                entity_deal_transform ON crm_deal_transform.ID = entity_deal_transform.entity_deal_ID
            WHERE
                crm_deal_transform.CATEGORY_ID = 42
            ORDER BY
                crm_deal_transform.ID ASC;""")
        try:
            result_Transactions_FPC = conn.execute(select_query_Transactions_FPC)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Transactions FPC exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Transactions FPC : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Transaction FPC
    if 'Transactions_Prise_en_charge_OPCO' in table_list_Transactions_final:
        # Vérifier si la table Transactions existe, si oui, la supprimer
        if 'Transactions_Prise_en_charge_OPCO' in table_names:
            conn.execute(text("DROP TABLE Transactions_Prise_en_charge_OPCO"))
            logging.info("Table Transactions_Prise_en_charge_OPCO supprimée avec succès.")


        #Insertion des données dans Transactions_Prise_en_charge_OPCO avec le SELECT
        #Les crm_deal_transform.CATEGORY_ID = 20 = Prise en charge OPCO
        select_query_Transactions_Prise_en_charge_OPCO = text("""
            CREATE TABLE Transactions_Prise_en_charge_OPCO
            ENGINE=MyISAM
            ROW_FORMAT=COMPRESSED
            KEY_BLOCK_SIZE=16
            AS
            SELECT
                crm_deal_transform.*,
                crm_deal_uf_transform.*,
                entity_deal_transform.*
            FROM
                crm_deal_transform
            LEFT JOIN
                crm_deal_uf_transform ON crm_deal_transform.ID = crm_deal_uf_transform.DEAL_ID
            LEFT JOIN
                entity_deal_transform ON crm_deal_transform.ID = entity_deal_transform.entity_deal_ID
            WHERE
                crm_deal_transform.CATEGORY_ID = 20
            ORDER BY
                crm_deal_transform.ID ASC;""")
        try:
            result_Transactions_Prise_en_charge_OPCO = conn.execute(select_query_Transactions_Prise_en_charge_OPCO)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Transactions_Prise_en_charge_OPCO exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Transactions_Prise_en_charge_OPCO : {e}")
    # Attend 5s avant de faire la suite
    sleep(5)
    logging.info("Le scrypt sur les différentes tables Transactions est terminé")


#------------------------------------------ Changement de kanban > Prospects --------------------------------------------------
Reconnexion()

if 'Prospects' in kanban_list_final:
    table_prioritaire_a_verifier_Prospects = ['crm_lead_transform']
    table_secondaire_a_verifier_Prospects = ['crm_lead_uf_transform', 'entity_lead_transform']
    doublon_Prospects = []


    query_table_prioritaire_doublon_Prospects = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_prioritaire_a_verifier_Prospects[0]}'")
    result_table_prioritaire_doublon_Prospects = conn.execute(query_table_prioritaire_doublon_Prospects)
    list_table_prioritaire_doublon_Prospects = [row[0] for row in result_table_prioritaire_doublon_Prospects if row[0] not in ['ID', 'DEAL_ID']]
    logging.info(f"Liste des doublons dans la table prioritaire : {list_table_prioritaire_doublon_Prospects}")

    #Supprimer les colonnes de "list_table_prioritaire_doublon" dans les tables secondaires
    #Si certaines données manquent, cette partie peut etre mis en pause afin de ravoir l'entièreté des données sur la BDD
    for table in table_secondaire_a_verifier_Prospects:
        for col in list_table_prioritaire_doublon_Prospects:
            try:
                # Vérifier d'abord si la colonne existe
                if conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE '{col}'")).fetchone():
                    conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {col}"))
                    logging.info(f"Colonne {col} supprimée de la table {table}")
            except Exception as e:
                logging.error(f"Erreur lors de la suppression de la colonne {col} de la table {table}: {e}")
    # Attend 5s avant de faire la suite
    sleep(5)


    #Création de la table Prospects
    if 'Prospects' in kanban_list_final:
        # Vérification et suppression des doublons dans les tables de prospects
        table_prioritaire_a_verifier_Prospects = ['crm_lead_transform']
        table_secondaire_a_verifier_Prospects = ['crm_lead_uf_transform', 'entity_lead_transform']
        doublon_Prospects = []

        query_table_prioritaire_doublon_Prospects = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_prioritaire_a_verifier_Prospects[0]}'")
        result_table_prioritaire_doublon_Prospects = conn.execute(query_table_prioritaire_doublon_Prospects)
        list_table_prioritaire_doublon_Prospects = [row[0] for row in result_table_prioritaire_doublon_Prospects if row[0] not in ['ID', 'LEAD_ID']]
        logging.info(f"Liste des doublons dans la table prioritaire : {list_table_prioritaire_doublon_Prospects}")

        for table in table_secondaire_a_verifier_Prospects:
            for col in list_table_prioritaire_doublon_Prospects:
                try:
                    if conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE '{col}'")).fetchone():
                        conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {col}"))
                        logging.info(f"Colonne {col} supprimée de la table {table}")
                except Exception as e:
                    logging.error(f"Erreur lors de la suppression de la colonne {col} de la table {table}: {e}")

        sleep(5)  # Attendre avant de continuer

    # Création de la table Prospects
    if 'Prospects' in table_list_Prospects_final:
        if 'Prospects' in table_names:
            conn.execute(text("DROP TABLE Prospects"))
            logging.info("Table Prospects supprimée avec succès.")

        select_query_Prospects = text("""
        CREATE TABLE Prospects
        ENGINE=InnoDB
        ROW_FORMAT=COMPRESSED
        KEY_BLOCK_SIZE=16
        AS
        SELECT
            crm_lead_transform.*,
            crm_lead_uf_transform.*,
            entity_lead_transform.*
        FROM
            crm_lead_transform
        JOIN
            crm_lead_uf_transform ON crm_lead_transform.ID = crm_lead_uf_transform.LEAD_ID
        JOIN
            entity_lead_transform ON crm_lead_transform.ID = entity_lead_transform.entity_lead_ID
        """)
        try:
            result_Prospects = conn.execute(select_query_Prospects)
            sleep(10) # Attend 10s avant de faire la suite
            logging.info("Création et requête d'insertion des données dans la table Prospects exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion des données dans la table Prospects : {e}")

        sleep(5)  # Attendre avant de continuer

    logging.info("Le script sur les différentes tables Prospects est terminé")


    Colonne_a_indexer2 = [
        ("Prospects", "ID"),
        ("Prospects", "CONTACT_ID"),
        ("Transactions", "LEAD_ID"),
        ("Transactions", "ID"),
        ("Contacts", "ID")]

    # Création des index si ils n'existent pas
    for table, column in Colonne_a_indexer2:
        try:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column})"))
            logging.info(f"Index créé pour la colonne {column} de la table {table}")
        except Exception as e:
            logging.error(f"Erreur lors de la création de l'index pour la colonne {column} de la table {table}: {e}")

    # Attend 5s avant de faire la suite
    sleep(5)

    Reconnexion()

    #Création de la table Prospects TC avec des colonnes de Transactions et Contacts
    if 'Prospects_TC' in table_list_Prospects_final:
        # Vérifier si la table Prospects TC existe, si oui, la supprimer
        if 'Prospects_TC' in table_names:
            conn.execute(text("DROP TABLE Prospects_TC"))
            logging.info("Table Prospects TC supprimée avec succès.")

        #Création de la table Prospects_TC avec des données de Transactions et Contacts
        try:
            # Ajout de nettoyage des tables pour les IDs non entiers
            try:
                # Supprimer les lignes de la table Prospects où ID n'est pas un entier
                conn.execute(text("DELETE FROM Prospects WHERE ID NOT REGEXP '^-?[0-9]+$'"))
                conn.execute(text("ALTER TABLE Prospects MODIFY ID INT"))
                conn.execute(text("UPDATE Prospects SET CONTACT_ID = NULL WHERE CONTACT_ID NOT REGEXP '^-?[0-9]+$'"))
                conn.execute(text("ALTER TABLE Prospects MODIFY CONTACT_ID INT"))
                # Mettre à jour les champs LEAD_ID dans la table Transactions
                conn.execute(text("UPDATE Transactions SET LEAD_ID = NULL WHERE LEAD_ID NOT REGEXP '^-?[0-9]+$'"))
                conn.execute(text("ALTER TABLE Transactions MODIFY LEAD_ID INT"))
                # Supprimer les lignes de la table Contacts où ID n'est pas un entier
                conn.execute(text("DELETE FROM Contacts WHERE ID NOT REGEXP '^-?[0-9]+$'"))
                conn.execute(text("ALTER TABLE Contacts MODIFY ID INT"))
            except Exception as e:
                logging.error(f"Erreur lors du nettoyage des tables : {e}")

            # Création de la table Prospects_TC
            conn.execute(text("CREATE TABLE Prospects_TC ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=16 AS SELECT * FROM Prospects"))

            # Index sur ID de Prospects_TC
            conn.execute(text("ALTER TABLE Prospects_TC ADD INDEX idx_ID_on_prospects_tc (ID)"))

            sleep(10)
            # Mise à jour de Prospects_TC en ajoutant des colonnes de Contacts
            try:
                conn.execute(text("""
                    ALTER TABLE Prospects_TC
                    ADD Contacts_EMAIL text,
                    ADD Contact_Source_Description text
                """))
                logging.info("Ajout des colonnes Contacts et Source description dans la table Prospects_TC réussi")
            except (OperationalError, MySQLOperationalError) as e:
                if 'Connexion perdu durant lenvoi de la requête' in str(e):
                    logging.error(f"Erreur lors de lajout des colonnes Contacts et Source description dans la table Prospects_TC : {e}")
                    # Tentez de vous reconnecter
                    conn = engine.connect()
                    # Réessayez la requête
                else:
                    raise

            sleep(10)
            try:
                conn.execute(text("""
                    UPDATE Prospects_TC PT
                    LEFT JOIN Contacts C ON PT.CONTACT_ID = C.ID
                    SET PT.Contacts_EMAIL = C.EMAIL,
                        PT.Contact_Source_Description = C.SOURCE_DESCRIPTION
                """))
                logging.info("Mise à jour des colonnes Contacts dans la table Prospects_TC réussi")
            except (OperationalError, MySQLOperationalError) as e:
                if 'Connexion perdu durant lenvoi de la requête' in str(e):
                    logging.error(f"Erreur lors de la mise à jour des colonnes Contacts dans la table Prospects_TC : {e}")
                    # Tentez de vous reconnecter
                    conn = engine.connect()
                    # Réessayez la requête
                else:
                    raise


            sleep(10)
            try:
                Reconnexion()
                conn.execute(text("""
                    ALTER TABLE Prospects_TC
                    ADD Transactions_SOURCE_NAME text,
                    ADD Transaction_Source_Description text,
                    ADD UF_CRM_5E5634910DD58 text,
                    ADD UF_CRM_1591275382 text,
                    ADD UF_CRM_5FA697664B231 text,
                    ADD UF_CRM_6341774C87D94 text
                """))
                logging.info("Ajout des colonnes Transactions dans la table Prospects_TC réussi")
            except (OperationalError, MySQLOperationalError) as e:
                if 'Connexion perdu durant lenvoi de la requête' in str(e):
                    logging.error(f"Erreur lors de lajout des colonnes Transactions dans la table Prospects_TC : {e}")
                    # Tentez de vous reconnecter
                    conn = engine.connect()
                    # Réessayez la requête
                else:
                    raise

            sleep(10)
            for _ in range(3):  # Réessayer jusqu'à 3 fois car certaines fois ca peut bug a cause de la surcharge du serveur de la BDD
                try:
                    Reconnexion()
                    conn.execute(text("""
                        UPDATE Prospects_TC PT
                        LEFT JOIN Transactions T ON PT.ID = T.LEAD_ID
                        SET PT.Transactions_SOURCE_NAME = T.SOURCE_NAME,
                            PT.Transaction_Source_Description = T.SOURCE_DESCRIPTION,
                            PT.UF_CRM_5E5634910DD58 = T.UF_CRM_5E5634910DD58,
                            PT.UF_CRM_1591275382 = T.UF_CRM_1591275382,
                            PT.UF_CRM_5FA697664B231 = T.UF_CRM_5FA697664B231,
                            PT.UF_CRM_6341774C87D94 = T.UF_CRM_6341774C87D94
                    """))
                    logging.info("Mise à jour des colonnes Transactions dans la table Prospects_TC réussi")
                    break  # Si la requête a réussi, sortir de la boucle
                except (OperationalError, MySQLOperationalError) as e:
                    if 'Connexion perdu durant lenvoi de la requête' in str(e):
                        logging.error(f"Erreur de connexion lors de la mise à jour des colonnes Transactions dans la table Prospects_TC : {e}")
                        # Tentez de vous reconnecter
                        conn = engine.connect()
                    else:
                        logging.error(f"Erreur lors de la mise à jour des colonnes Transactions dans la table Prospects_TC : {e}")
                        raise  # Si l'erreur n'est pas une erreur de connexion, lever l'exception


            logging.info("Création et transformation pour la table Prospects TC exécutée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de la création et la transformation dans la table Prospects TC : {e}")



#------------------------------------------ Changement de kanban > Entreprise --------------------------------------------------
Reconnexion()

if 'Entreprise' in table_list_Entreprise_final:
    # Vérification et suppression des doublons dans les tables d'entreprise
    table_prioritaire_a_verifier_Entreprise = ['crm_company_transform']
    table_secondaire_a_verifier_Entreprise = ['crm_company_uf_transform', 'entity_company_transform']
    doublon_Entreprise = []

    query_table_prioritaire_doublon_Entreprise = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_prioritaire_a_verifier_Entreprise[0]}'")
    result_table_prioritaire_doublon_Entreprise = conn.execute(query_table_prioritaire_doublon_Entreprise)
    list_table_prioritaire_doublon_Entreprise = [row[0] for row in result_table_prioritaire_doublon_Entreprise if row[0] not in ['ID', 'COMPANY_ID']]
    logging.info(f"Liste des doublons dans la table prioritaire : {list_table_prioritaire_doublon_Entreprise}")

    # Supprimer les colonnes de "list_table_prioritaire_doublon" dans les tables secondaires
    #Si certaines données manquent, cette partie peut etre mis en pause afin de ravoir l'entièreté des données sur la BDD
    for table in table_secondaire_a_verifier_Entreprise:
        for col in list_table_prioritaire_doublon_Entreprise:
            try:
                if conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE '{col}'")).fetchone():
                    conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {col}"))
                    logging.info(f"Colonne {col} supprimée de la table {table}")
                else:
                    logging.info(f"La colonne {col} n'existe pas dans la table {table}")
            except Exception as e:
                logging.error(f"Erreur lors de la suppression de la colonne {col} de la table {table}: {e}")

    sleep(5)  # Attendre avant de continuer

    # Création de la table Entreprise
    if 'Entreprise' in table_names:
        conn.execute(text("DROP TABLE IF EXISTS Entreprise"))
        logging.info("Table Entreprise supprimée avec succès.")

    select_query_Entreprise = text("""
    CREATE TABLE Entreprise
    ENGINE=InnoDB
    ROW_FORMAT=COMPRESSED
    KEY_BLOCK_SIZE=16
    AS
    SELECT
        crm_company_transform.*,
        crm_company_uf_transform.*,
        entity_company_transform.*
    FROM
        crm_company_transform
    JOIN
        crm_company_uf_transform ON crm_company_transform.ID = crm_company_uf_transform.COMPANY_ID
    JOIN
        entity_company_transform ON crm_company_transform.ID = entity_company_transform.entity_company_ID
    """)
    try:
        result_Entreprise = conn.execute(select_query_Entreprise)
        logging.info("Création et requête d'insertion des données dans la table Entreprise exécutée avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'insertion des données dans la table Entreprise : {e}")

    sleep(5)  # Attendre avant de faire la suite

logging.info("Le script sur les différentes tables Entreprise est terminé")

# Fermer toutes les connexions
engine.dispose()
logging.info("Toutes les connexions au pool ont été fermées et le scrypt sur les différentes tables est terminé")
