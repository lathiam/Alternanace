import time
import numpy as np
import requests
import re
import json
import os
import pandas as pd
import datetime
import logging
import traceback

from collections import Counter
from sqlalchemy import Integer, Float, String, DateTime, Text, Column, MetaData, Table, create_engine, inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from pandas.api.types import is_datetime64_any_dtype
from dateutil import parser
from logging.handlers import RotatingFileHandler
from dateutil.parser import parse
from itertools import cycle
from Colonnes_a_exclure_ou_inclure_de_la_BDD import Colonnes_a_exclure, Colonnes_a_inclure_facture, colonnes_a_exclure_de_crm_deal_uf
from Cles_API_et_BDD import DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_CHARSET, API_SECRETS, API_REQUEST_DELAY_ANGERS, API_REQUEST_DELAY_NANTES, API_REQUEST_DELAY_AUTRES


def configure_logging():
    # Configuration du logger SQL Alchemy
    sqlalchemy_log_handler = RotatingFileHandler('Logs/Logs_ETL_Principal_sqlalchemy.txt', maxBytes=300000, backupCount=1)
    sqlalchemy_log_handler.setLevel(logging.DEBUG)
    sqlalchemy_log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sqlalchemy_log_handler.setFormatter(sqlalchemy_log_formatter)
    sqlalchemy_logger = logging.getLogger('sqlalchemy')
    sqlalchemy_logger.setLevel(logging.INFO)
    sqlalchemy_logger.addHandler(sqlalchemy_log_handler)
    sqlalchemy_logger.propagate = False

    # Configuration du logger général pour fichier
    general_log_handler = RotatingFileHandler('Logs/Logs_ETL_Principal_general.txt', maxBytes=3000000, backupCount=1)
    general_log_handler.setLevel(logging.DEBUG)
    general_log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    general_log_handler.setFormatter(general_log_formatter)
    general_logger = logging.getLogger()
    general_logger.setLevel(logging.INFO)
    general_logger.addHandler(general_log_handler)

    # Configuration du StreamHandler pour afficher les logs "Général" dans le terminal
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)  # Affiche seulement les logs de niveau INFO et supérieur
    stream_handler.setFormatter(general_log_formatter)  # Utilise le même format que le logger général
    general_logger.addHandler(stream_handler)  # Ajout du handler au logger général

# Appeler la fonction de configuration de log
configure_logging()



class DataFetcher:
    GET = "GET"
    POST = "POST"

    def __init__(self, database_name):
        self.api_keys = cycle(API_SECRETS[database_name])
        self.total_entries = None
        if database_name == 'bitrix_angers':
            self.api_delay = API_REQUEST_DELAY_ANGERS
        elif database_name == 'bitrix_nantes':
            self.api_delay = API_REQUEST_DELAY_NANTES
        else:
            self.api_delay = API_REQUEST_DELAY_AUTRES  # Un délai par défaut pour toute autre base de données
        logging.info(f"Initialisation de DataFetcher pour {database_name} avec un délai de {self.api_delay} secondes.")


    def _make_request(self, method, endpoint, body=None):
        api_key = next(self.api_keys)  # Obtenir la clé API suivante
        url = f"{api_key}{endpoint}"
        logging.debug(f"Début de la requête {method} vers {url} avec le corps {body}")
        time.sleep(self.api_delay)  # Respecter le délai entre les requêtes

        try:
            if method == self.GET:
                response = requests.get(url)
            elif method == self.POST:
                response = requests.post(url, json=body)

            response.raise_for_status()  # Lève une exception pour les réponses HTTP avec erreur
            logging.debug(f"Réponse reçue de {url} avec le statut {response.status_code}")
            return self._parse_response(response)
        except requests.exceptions.HTTPError as e:
            logging.error(f"Erreur HTTP lors de la requête vers {url}: {e.response.status_code} - {e.response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur de requête vers {url}: {e}")
        except Exception as e:
            logging.error(f"Erreur inattendue lors de la requête vers {url}: {e}")
        return None

    def clean_data(value):
        if value == "non sélectionné":
            return None  # Ou toute autre valeur que vous souhaitez utiliser à la place
        return value

    def _parse_response(self, response):
        try:
            data = response.json()
            if 'total' in data:
                self.total_entries = data['total']
            if 'result' in data:
                return data['result']
            else:
                logging.warning(f"La réponse de {response.url} ne contient pas de 'result'.")
        except ValueError:
            logging.error(f"Erreur lors de l'analyse de la réponse JSON de {response.url}: {response.text}")
        return None


    def _fetch_paged_data(self, endpoint=None, body=None, entity_type_id=None, start=None):
        data_list = []
        last_id = 0
        seen_ids = set()

        while True:
            if entity_type_id:
                endpoint = f"crm.item.list"
                body = {
                    "entityTypeId": entity_type_id,
                    "order": {"id":"ASC"},
                    "filter": {">id": last_id},
                    "start": -1
                }
                results = self._make_request(self.POST, endpoint,body)

                results = results["items"]
            elif start is not None:
                results = self._make_request(self.POST, endpoint, body)
                # Supposons que 'results' est une liste de dictionnaires contenant un champ 'ID'
                for result in results:
                    new_id = result.get('ID')  # Remplacez 'ID' par le nom réel du champ
                    if new_id in seen_ids:
                        break  # Arrêter la boucle for
                    else:
                        seen_ids.add(new_id)

                else:  # Ce bloc s'exécute si la boucle for se termine normalement (pas de 'break')
                    start = start + 50
                    body["start"] = start + 50
                    data_list.extend(results)
                    continue  # Continuer la boucle while
                break

            else:
                body["filter"].update({">ID": last_id})
                results = self._make_request(self.POST, endpoint, body)


            if not results:
                break

            data_list.extend(results)

            try:
                if entity_type_id:
                    last_id = int(results[-1]['id'])
                else:
                    last_id = int(results[-1]['ID'])
            except:
                break

        if body:
            if "start" in body:
                del body["start"]

            if "filter" in body:
                del body["filter"]
        else:
            body = {}

        results = self._make_request(self.POST, endpoint, body)
        return data_list

    def clean_data(value):
        if value == "non sélectionné":
            return None  # Ou toute autre valeur que vous souhaitez utiliser à la place
        return value


    def fetch_crm_fields(self, entity_type):
        fields = self._make_request(self.GET, f"crm.{entity_type}.fields")
        if fields:
            return [key for key, value in fields.items() if value['type'] in ['crm', 'crm_entity', 'iblock_section','iblock_element']]
        return []

    def fetch_data_lists(self, list_id):
        data_list = []
        last_id = 0
        endpoint = "lists.element.get"

        while True:
            body = {
                "IBLOCK_ID": list_id,
                "IBLOCK_TYPE_ID": "lists",
                "filter": {">ID": last_id},
                "start": -1
            }

            results = self._make_request("POST", endpoint, body)

            if results:
                for item in results:
                    # Extract only value field from property fields
                    for key, value in item.items():
                        if key.startswith('PROPERTY_') and isinstance(value, dict):
                            item[key] = list(value.values())[0]

                data_list.extend(results)
                if results:
                    last_id = int(results[-1]['ID'])
                else:
                    break
            else:
                break

        del body["start"]
        del body["filter"]

        results = self._make_request("POST", endpoint, body)

        return data_list

    def clean_table_name(name):
        return re.sub(r'[^\w]', '', name)

    def fetch_bitrix_lists(self):
        data_list = {}

        endpoint = "lists.get"
        body = {
            "IBLOCK_TYPE_ID": "lists"
        }

        results = self._make_request("POST", endpoint, body)

        if results:
            for item in enumerate(results):
                name = DataFetcher.clean_table_name(item[1]['NAME'].replace(" ", "_"))
                data_list[name] = item[1]['ID']
        else:
            self._log_error(f"Unexpected response: {results}")

        return data_list

    def fetch_product_row_data(self, owner_type, owner_id):
        data_list = []
        last_id = 0

        while True:
            endpoint = "crm.item.productrow.list"
            body = {
                "order": {"ownerId": "ASC"},
                "filter": {
                    "=ownerType": owner_type,
                    ">ownerId": last_id,
                    "=ownerId": owner_id
                },
                "start": -1
            }
            results = self._make_request("POST", endpoint, body)

            if results and 'productRows' in results:
                data_list.extend(results['productRows'])
                if results['productRows']:
                    last_id = int(results['productRows'][-1]['ownerId'])
                else:
                    break
            else:
                self._log_error(f"Unexpected response: {results}")
                break

        del body["filter"][">ownerId"]
        del body["start"]
        results = self._make_request("POST", endpoint, body)

        return data_list

    def fetch_data_by_entity_id(self, type_id):
        return self._fetch_paged_data(entity_type_id=type_id)

    def fetch_data_pbi(self, list_name, database_name):
        if database_name == 'bitrix_angers':
            response = requests.get(f'https://espl.bitrix24.fr/bitrix/tools/biconnector/pbi.php?token=3wlPq8TUSGILuQO3SX87wKXzdbWOivCMfr&table={list_name}')
        elif database_name == 'bitrix_nantes':
            response = requests.get(f'https://cie-formation.bitrix24.fr/bitrix/tools/biconnector/pbi.php?token=6mxb5QbkEM2YgAq4EZAZUY3raZLmFn9tfr&table={list_name}')
        http_response = response.json()
        self.total_entries = len(http_response) - 1
        return http_response

    def fetch_and_create_dataframe(self, list_name, database_name):
        raw_data = self.fetch_data_pbi(list_name, database_name)
        df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        return df

    def fetch_data_by_entity(self, entity_type, crm_fields):
        logging.info(f"Récupération des données pour l'entité {entity_type} avec les champs {crm_fields}")
        body = {
            "select": crm_fields,
            "order": {"ID": "ASC"},
            "filter": {},
            "start": -1
        }
        return self._fetch_paged_data(f"crm.{entity_type}.list", body)

    def fetch_data_catalog_product(self, entity_type, crm_fields):
        start = 0
        body = {
            "order": {"ID": "ASC"},
            "start": start
        }
        return self._fetch_paged_data(f"crm.{entity_type}.list", body,'',start)

    def fetch_product_data(self):
        endpoint = "crm.product.list"
        body = {
            "select": ["PROPERTY_*", "*"],
            "order": {"ID": "ASC"},
            "filter": {},
            "start": -1
        }
        raw_data = self._fetch_paged_data(endpoint, body)
        return [self._process_product_item(item) for item in raw_data]

    def _process_product_item(self, item):
        for key, value in item.items():
            if key.startswith('PROPERTY_') and isinstance(value, dict) and 'value' in value:
                item[key] = value['value']
        return item

    def fetch_entity_type_ids(self):
        endpoint = "crm.type.list"
        data = self._make_request(self.GET, endpoint)

        if 'types' in data:
            return [type_info['entityTypeId'] for type_info in data['types']]
        return []

    def _log_error(self, error_message):
        with open('log.txt', 'a') as f:
            f.write(f"[DataFetcher Error]: {error_message}\n")


class DataProcessor:

    @staticmethod
    def clean_specific_values(df):
        return df.replace("non sélectionné", None)

    @staticmethod
    def replace_empty_with_null(df):
        # Utiliser la méthode replace pour remplacer les valeurs vides par None
        df = df.replace(r'^\s*$', None, regex=True)
        # Utiliser infer_objects pour conserver le comportement actuel de réduction de type
        df = df.infer_objects(copy=False)
        return df



    @staticmethod
    def process_data(data_list):
        df = pd.DataFrame(data_list)
        date_cols = []
        for col in df.columns:
            df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[col] = df[col].apply(DataProcessor.format_value)
            if df[col].dtype == 'datetime64[ns]':
                date_cols.append(col)
        return df, date_cols


    #def replace_nulls_with_zeros(df):
        for col in df.columns:
            non_null_values = df[col].dropna()

            # Vérifie si la majorité des valeurs non nulles sont des nombres
            if all(isinstance(x, (int, float)) for x in non_null_values):
                df[col] = df[col].fillna(0)
        return df

    @staticmethod
    def process_product_data(data_list):
        df = pd.DataFrame(data_list)
        for col in df.columns:
            df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[col] = df[col].apply(DataProcessor.format_value)
        return df

    @staticmethod
    def process_pbi_data(data_list):
        df = pd.DataFrame(data_list)
        for col in df.columns:
            df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            df[col] = df[col].apply(DataProcessor.format_value_pbi)
        return df

    @staticmethod
    def format_value_pbi(value):
        if pd.isna(value):
            return ''  # ou une autre valeur par défaut
        return value

    @staticmethod
    def format_value(value):
        if value is None:
            return None
        if value is False:
            return 0
        if isinstance(value, list):
            return json.dumps(value)
        if isinstance(value, str):
            value = value.replace(u'\ufeff', '').encode('utf-8', 'ignore').decode('utf-8')
            if '|EUR' in value:
                value = re.sub(r'[^0-9.]+', '', value)
            elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$', value):
                try:
                    dt = parser.parse(value)  # Replace pd.to_datetime(...) with parser.parse(...)
                    dt_no_tz = dt.replace(tzinfo=None)  # Add this line

                    if dt.year > 2262 or dt.year < 1677:
                        value = None  # Replace out-of-bounds dates with None
                    else:
                        value = dt_no_tz  # Replace dt with dt_no_tz
                except ValueError:
                    value = None  # Replace invalid dates with None

        return value

    @staticmethod
    def filter_numeric(df, active=True):
        if active:
            df = df.select_dtypes(include=[pd.np.number])
        return df

    def _log_error(self, error_message):
        with open('log.txt', 'a') as f:
            f.write(f"[DataProcessor Error]: {error_message}\n")


class DatabaseHandler:

    def __init__(self, username, password, database_name):
        self.database_connection = create_engine(
            f'mysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOSTNAME}/{database_name}?charset=utf8',
            connect_args={'sql_mode':'ALLOW_INVALID_DATES'},
            pool_size=10,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=800
        )


    def ping_database(self):
        try:
            with self.database_connection.connect() as connection:
                connection.execute(text('SELECT 1'))
                logging.debug('Ping réussi à la base de données.')
        except Exception as e:
            logging.error(f'Erreur lors du ping à la base de données: {str(e)}')
            self.reconnect_database()

    def reconnect_database(self):
        try:
            self.database_connection = self.create_database_connection()
            logging.info('Reconnexion à la base de données réussie.')
        except Exception as e:
            logging.error(f'Erreur lors de la tentative de reconnexion à la base de données: {str(e)}')


    def get_entity_ids(self, entity):
        with self.database_connection.begin() as connection:
            result = connection.execute(text(f"SELECT id FROM {entity} WHERE `opportunity` > 0"))
            return [row[0] for row in result.fetchall()]

    def dynamic_table_insert(self, df, list_name, primary_key='ID'):
        # Traitement des données
        # Assumant que les méthodes suivantes existent dans DataProcessor
        df = DataProcessor.process_pbi_data(df)
        #df = DataProcessor.replace_nulls_with_zeros(df)

        Base = declarative_base()
        metadata = MetaData()
        metadata.reflect(self.database_connection)


        def is_date(string, fuzzy=False):
            try:
                # Use regex to check for date format
                if re.match(r"^\d{4}-\d{2}-\d{2}$", string) or re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[\+\-]\d{2}:\d{2})$", string):
                    return True
                parse(string, fuzzy=fuzzy)
                return True
            except:
                return False


        def detect_majority_type(col_name, series):
            # Si toutes les valeurs sont NULL ou des chaînes vides, retourner Text
            if (pd.isna(x) or x == "" for x in series):
                return Text

            # Supprimer les valeurs nulles et les chaînes vides
            non_empty_series = series.dropna().loc[series != '']

            # Essayer de détecter les dates
            if all(is_date(str(x)) for x in non_empty_series):
                return DateTime


            # Vérifier si au moins une valeur commence par "0" et a une longueur > 1
            if any(str(x).startswith('0') and len(str(x)) > 1 for x in non_empty_series):
                return Text

            # Essaye de convertir les chaînes en nombres
            converted_series = pd.to_numeric(non_empty_series, errors='coerce')

            # Si au moins une conversion échoue (au moins un élément est NaN), retourner Text
            if any(pd.isna(x) for x in converted_series):
                return Text

            # Filtrer les valeurs qui sont soit nulles soit des chaînes vides
            filtered_series = converted_series.dropna()

            # Compte les types des valeurs restantes
            types_counter = Counter([type(x) for x in filtered_series])

            # Retourne Text si toutes les valeurs sont nulles ou des chaînes vides
            if not types_counter:
                return Text

            most_common_type, _ = types_counter.most_common(1)[0]

            if most_common_type == int:
                return Integer
            elif most_common_type == float:
                return Float
            else:
                return Text

        column_definitions = {}
        df = df.replace({"0000-00-00 00:00:00": None})
        for col in df.columns:
            sql_type = detect_majority_type(col, df[col])
            if col == primary_key:
                column_definitions[col] = Column(Integer, primary_key=True)
            else:
                column_definitions[col] = Column(sql_type)

        DynamicTable = type('DynamicTable', (Base,), {
            '__tablename__': list_name,
            **column_definitions
        })

        # Vérifier si les tables du df sont dans la base de données
        if list_name in metadata.tables:
            table = Table(list_name, metadata, autoload=True, autoload_with=self.database_connection)
            with self.database_connection.connect() as connection:
                connection.execute(text(f'TRUNCATE TABLE {list_name}'))
        else:
            # Création de la table avec ENGINE=MyISAM pour crm_deal_uf
            if list_name == 'crm_deal_uf':
                with self.database_connection.connect() as connection:
                    connection.execute(text(f"""
                        CREATE TABLE {list_name} (
                            {', '.join([f'{col.name} {col.type}' for col in column_definitions.values()])},
                            PRIMARY KEY ({primary_key})
                        ) ENGINE=MyISAM ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=16;
                    """))
                    logging.info(f"Table {list_name} créée avec succès avec le moteur MyISAM.")
            else:
                Base.metadata.create_all(self.database_connection)
                logging.info(f"Table {list_name} créée avec succès dans la base de données.")

            # Modifier le format de ligne pour toutes les tables
            with self.database_connection.connect() as connection:
                connection.execute(text(f"ALTER TABLE {list_name} ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=16;"))
                logging.info(f"Format de ligne de la table {list_name} changé en COMPRESSED avec KEY_BLOCK_SIZE=16.")

        # Vérifier si les colonnes du df sont dans la base de données
        inspector = inspect(self.database_connection)
        existing_columns = [column_info['name'] for column_info in inspector.get_columns(list_name)]
        for col in df.columns:
            if col not in existing_columns:
                with self.database_connection.begin() as connection:
                    # Déterminer le type de données SQL en fonction du type de données Python
                    sql_data_type = self._get_sql_data_type(df[col].dtype.type)
                    # Ajouter la colonne à la table
                    connection.execute(text(f"ALTER TABLE {list_name} ADD {col} {sql_data_type};"))
                    logging.info(f'Colonne {col} créée avec succès dans la table {list_name}.')


        #Injection en masse des données
        Session = sessionmaker(bind=self.database_connection)
        session = Session()
        # Par cette ligne
        session.bulk_insert_mappings(DynamicTable, df.replace({np.nan: None}).to_dict(orient="records"))
        session.commit()

    def truncate_product_row_data(self, table_name):
        try:
            inspector = inspect(self.database_connection)
            if inspector.has_table(table_name):
                self._truncate_table(table_name)
        except Exception as e:
            logging.error(f'Error occurred: {str(e)}\n')


    def insert_data(self, df, date_cols, entity_type_id, total_entries=None ):
        table_name = f"entity_{entity_type_id}"  # Assurez-vous que table_name est défini au début
        try:
            if df.empty == False:
                inspector = inspect(self.database_connection)

                # Supprimer les tables spécifiées avant la création/insertion
                if table_name in ['entity_deal', 'entity_contact', 'entity_lead', 'entity_company']:
                    with self.database_connection.begin() as connection:
                        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        logging.info(f"Table {table_name} supprimée avec succès.")

                # Si entity_type_id est 31 (facture), vérifiez et filtrez les colonnes à inclure spécifiées
                if entity_type_id == 31 and database_name == 'bitrix_angers':
                    colonnes_non_existantes_31 = [col for col in Colonnes_a_inclure_facture if col not in df.columns]
                    if colonnes_non_existantes_31:
                        logging.info(f"Les colonnes suivantes spécifiées pour inclusion n'existent pas dans le DataFrame pour {table_name}: {colonnes_non_existantes_31}")
                    # Filtrez df pour ne conserver que les colonnes existantes
                    colonnes_inclure_filtrees_facture = [col for col in Colonnes_a_inclure_facture if col in df.columns]
                    df = df[colonnes_inclure_filtrees_facture]

                # Filtrer les colonnes à exclure pour les entity_type_id
                columns_to_exclude = Colonnes_a_exclure.get(entity_type_id, [])
                df = df.drop(columns=columns_to_exclude, errors='ignore')  # Utilisez errors='ignore' pour éviter des erreurs si une colonne n'existe pas

                if inspector.has_table(table_name):
                    self._truncate_table(entity_type_id)
                    existing_columns = [column_info['name'] for column_info in inspector.get_columns(table_name)]
                    for col in df.columns:
                        if col not in existing_columns:
                            with self.database_connection.begin() as connection:
                                # Determine SQL data type based on Python data type
                                sql_data_type = self._get_sql_data_type(df[col].dtype.type)
                                # Add column to the table
                                connection.execute(text(f"ALTER TABLE {table_name} ADD {col} {sql_data_type};"))
                df.to_sql(con=self.database_connection, name=table_name, if_exists='append', index=False)
                self._convert_table_to_utf8mb4(table_name)
                self._add_primary_key_if_not_exists(inspector, entity_type_id)
                self._convert_to_datetime(date_cols, entity_type_id)
                self._log_activity(self.database_connection.url.database, table_name, df.shape[0], total_entries)
        except Exception as e:
            logging.error(f'Errors function insert_data occurred: {str(e)}\n')
            self._log_activity(self.database_connection.url.database, table_name, df.shape[0], total_entries, str(e))

    def _get_sql_data_type(self, python_data_type):
        if python_data_type in [int, float, np.float64, np.int64]:
            return "DOUBLE"
        elif python_data_type == str:
            return "TEXT"
        else:
            return "TEXT"  # Default to TEXT

    def _truncate_table(self, entity_type_id):
        with self.database_connection.begin() as connection:
            connection.execute(text(f"TRUNCATE TABLE entity_{entity_type_id}"))

    def _truncate_logs(self):
        with self.database_connection.begin() as connection:
            connection.execute(text(f"TRUNCATE TABLE synchronization_log"))

    def _add_primary_key_if_not_exists(self, inspector, entity_type_id):
        table_name = f"entity_{entity_type_id}"
        primary_keys = inspector.get_pk_constraint(table_name)['constrained_columns']

        column_names = [column['name'] for column in inspector.get_columns(table_name)]
        id_column = 'id' if 'id' in column_names else 'ID'

        if id_column not in primary_keys:
            try:
                with self.database_connection.begin() as connection:
                    connection.execute(text(f"ALTER TABLE {table_name} MODIFY {id_column} INT;"))  # Convert id to INT before adding primary key
                    connection.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({id_column});"))
            except Exception as e:
                logging.error(f'Error occurred when trying to add primary key: {str(e)}\n')

    def _convert_to_datetime(self, date_cols, entity_type_id):
        with self.database_connection.begin() as connection:
            for col_name in date_cols:
                connection.execute(text(f"ALTER TABLE entity_{entity_type_id} MODIFY {col_name} DATETIME;"))

    def _convert_table_to_utf8mb4(self, table_name):
        try:
            with self.database_connection.begin() as connection:
                connection.execute(text(f"ALTER TABLE {table_name} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"))
        except Exception as e:
            logging.error(f'Error occurred when trying to convert table to utf8mb4_general_ci: {str(e)}\n')

    def _log_error(self, error_message):
        logging.error(f"[DatabaseHandler Error]: {error_message}")
        logging.error(traceback.format_exc())  # Log traceback information

    def _log_activity(self, database_name, table_name, entry_number,total,error_message=None):
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"{database_name} {table_name} Nombre: {entry_number} CRM: {total}"
        self.log_sync_event(current_time,database_name,table_name,entry_number,total,error_message)
        logging.info(log_entry)

    def log_sync_event(self, timestamp, database_name, table_name, db_entry_count, crm_entry_count, error_message=None):
        data = {
            'timestamp': timestamp,
            'database_name': database_name,
            'table_name': table_name,
            'db_entry_count': db_entry_count,
            'crm_entry_count': crm_entry_count,
            'error_message': error_message
        }

        # Vérifier si la table "synchronization_log" existe, sinon la créer
        inspector = inspect(self.database_connection)
        if 'synchronization_log' not in inspector.get_table_names():
            query = text("""
            CREATE TABLE synchronization_log (
                timestamp DATETIME,
                database_name VARCHAR(30),
                table_name VARCHAR(50),
                db_entry_count INT,
                crm_entry_count INT,
                error_message TEXT
            )
            """)
            try:
                with self.database_connection.begin() as connection:
                    connection.execute(query)
                logging.info("Table 'synchronization_log' created successfully.")
            except Exception as e:
                logging.error(f'Error creating table synchronization_log: {str(e)}\n')

        # Préparer la requête d'insertion
        query = text("""
        INSERT INTO synchronization_log (timestamp, database_name, table_name, db_entry_count, crm_entry_count, error_message)
        VALUES (:timestamp, :database_name, :table_name, :db_entry_count, :crm_entry_count, :error_message)
        """)

        # Insérer les données
        try:
            with self.database_connection.begin() as connection:
                connection.execute(query, data)
        except Exception as e:
            logging.error(f'Log error: {str(e)}\n')





filter_numeric_data = False
print("Lancement du scrypt")


for database_name in API_SECRETS.keys():  # Loop over each database
    logging.info(f"Traitement de la base de données {database_name}")
    try:
        data_fetcher = DataFetcher(database_name)  # Use the corresponding API secret
        data_processor = DataProcessor()
        database_handler = DatabaseHandler(DB_USERNAME, DB_PASSWORD, database_name)  # Create new DatabaseHandler for each database
        database_handler._truncate_logs()
        logging.info(f"Traitement réussi de la base de données {database_name}")
    except Exception as e:
        logging.error(f"Erreur lors de l'initialisation des handlers pour {database_name}: {str(e)}")
        continue  # Skip to next database if initialization fails

    # Supprimer les tables spécifiées avant le traitement
    tables_to_delete = ['entity_deal', 'entity_contact', 'entity_lead', 'entity_company']
    for table in tables_to_delete:
        try:
            with database_handler.database_connection.begin() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
                logging.info(f"Table {table} supprimée avec succès dans {database_name} afin que le script de renommage fonctionne correctement.")
        except Exception as e:
            logging.error(f"Erreur lors de la suppression de la table {table} dans {database_name}: {str(e)}")

    # #Devis
    # logging.info(f"Récupération des devis pour {database_name}")
    # try :
    #     quote_data_list = data_fetcher.fetch_data_by_entity("quote",["*","UF_*"])  # Fetch company data using company fields
    #     logging.info(f"TEST 1 {database_name}")
    #     df = data_processor.process_product_data(quote_data_list)
    #     logging.info(f"TEST 2 {database_name}")
    #     database_handler.ping_database()
    #     logging.info(f"TEST 3 {database_name}")
    #     database_handler.insert_data(df, [], 'quote',data_fetcher.total_entries)  # Insert quote data, assuming no date columns
    #     logging.info(f"TEST 4 {database_name}")
    #     data_fetcher.total_entries = None
    #     logging.info(f"Traitement réussi des devis pour {database_name}")
    # except Exception as e:
    #     logging.error(f'Erreur lors de la récupération des données des devis pour {database_name}: {str(e)}')

    #Factures Angers
    #if database_name == 'bitrix_angers':
        #logging.info(f"Récupération des factures pour {database_name}")
        #try:
            #database_handler.ping_database()
            #invoice_data_list = data_fetcher.fetch_data_by_entity("invoice",["*","UF_*"])  # Fetch company data using company fields
            #df = data_processor.process_product_data(invoice_data_list)  # Process quote data (assuming it has similar structure as product data)
            #database_handler.ping_database()
            #database_handler.insert_data(df, [], 'invoice',data_fetcher.total_entries)  # Insert quote data, assuming no date columns
            #data_fetcher.total_entries = None
            #logging.info(f"Traitement réussi des factures pour {database_name}")
        #except Exception as e:
            #logging.error(f'Erreur lors de la récupération des données des factures pour {database_name}: {str(e)}')


    #Produits
    logging.info(f"Récupération des produits pour {database_name}")
    try:
        logging.info(f"Tentative de récupération des données de produit pour {database_name}")
        product_data_list = data_fetcher.fetch_product_data()
        logging.info(f"Traitement des données de produit pour {database_name}")
        df = data_processor.process_product_data(product_data_list)
        df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
        df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
        logging.info(f"Ping de la base de données pour {database_name}")
        database_handler.ping_database()
        logging.info(f"Insertion des données de produit pour {database_name}")
        database_handler.insert_data(df, [], 'product', data_fetcher.total_entries)
        data_fetcher.total_entries = None
        logging.info(f"Traitement réussi des produits pour {database_name}")
    except Exception as e:
        logging.error(f'Erreur lors de la récupération des données de produit pour {database_name}: {str(e)}')

    if database_name == 'bitrix_angers':
        logging.info(f"Récupération des produits pour {database_name}")
        try:
            logging.info(f"Tentative de récupération des données de catalogue de produit pour {database_name}")
            product_catalog_list = data_fetcher.fetch_data_catalog_product("productsection", [])
            logging.info(f"Traitement des données de catalogue de produit pour {database_name}")
            df = data_processor.process_product_data(product_catalog_list)
            df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
            df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
            logging.info(f"Ping de la base de données pour {database_name}")
            database_handler.ping_database()
            logging.info(f"Insertion des données de catalogue de produit pour {database_name}")
            database_handler.insert_data(df, [], 'productsection', data_fetcher.total_entries)
            data_fetcher.total_entries = None
            logging.info(f"Traitement réussi des produits pour {database_name}")
        except Exception as e:
            logging.error(f"Erreur lors de l'actualisation des produits pour {database_name}: {str(e)}")

    if database_name == 'bitrix_nantes':
        logging.info(f"Récupération des produits pour {database_name}")
        try:
            database_handler.ping_database()
            for entity, owner_type in [('entity_140', 'T8c'), ('entity_160', 'Ta0')]: #AJouter ici si les entity où l'on souhaite récupérer les produits (entity, ownertype)
                logging.info(f"Traitement de l'entité {entity} pour {database_name}")
                owner_ids = database_handler.get_entity_ids(entity)
                product_row_data = data_fetcher.fetch_product_row_data(owner_type, owner_ids)
                df, date_cols = data_processor.process_data(product_row_data)
                df = data_processor.filter_numeric(df, filter_numeric_data)
                df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
                df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
                database_handler.ping_database()
                database_handler.insert_data(df, [], f"product_{entity}", data_fetcher.total_entries)
                data_fetcher.total_entries = None
                logging.info(f"Traitement réussi de l'entité {entity} pour {database_name}")
        except Exception as e:
            logging.error(f'Erreur lors de la récupération des données des produits pour {database_name}: {str(e)}')

    #Listes
    logging.info(f"Récupération des listes pour {database_name}")
    try:
        bitrixList = data_fetcher.fetch_bitrix_lists()
        logging.info(f"Récupération réussie des listes pour {database_name}")
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des listes pour {database_name}: {str(e)}")
        continue
    logging.info(f"Toutes les listes de Bitrix : {bitrixList}")

    for listName in bitrixList:
        logging.info(f"Traitement de la liste {listName} pour {database_name}")
        try:
            result = data_fetcher.fetch_data_lists(bitrixList[listName])
            df = data_processor.process_product_data(result)
            df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
            df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
            database_handler.ping_database()
            database_handler.insert_data(df, [], listName, data_fetcher.total_entries)
            data_fetcher.total_entries = None
            logging.info(f"Traitement réussi de la liste {listName} pour {database_name}")
        except Exception as e:
            logging.error(f'Erreur lors de la récupération des données des listes {listName} pour {database_name}: {str(e)}')


    #SPA
    logging.info(f"Récupération des SPA pour {database_name}")
    try:
        entity_type_ids = data_fetcher.fetch_entity_type_ids()
        entity_type_ids.append(31)
        logging.info(f"Récupération réussie des SPA pour {database_name}")
        logging.info(f"Tous les SPA de Bitrix : {entity_type_ids}")
    except Exception as e:
        logging.error(f'Erreur lors de la récupération des identifiants de type dentité pour {database_name}: {str(e)}')
        continue

    for type_id in entity_type_ids:
        logging.info(f"Traitement du type d'entité {type_id} pour {database_name}")
        try:
            database_handler.ping_database()
            data_list = data_fetcher.fetch_data_by_entity_id(type_id)
            df, date_cols = data_processor.process_data(data_list)
            df = data_processor.filter_numeric(df, filter_numeric_data)
            df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
            df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
            database_handler.ping_database()
            database_handler.insert_data(df, date_cols, type_id, data_fetcher.total_entries)
            data_fetcher.total_entries = None
            logging.info(f"Traitement réussi du type d'entité {type_id} pour {database_name}")
        except Exception as e:
            logging.error(f'Erreur lors de la récupération des données du SPA {type_id} pour {database_name}: {str(e)}')



    #Entity_Transactions (Table avec toutes les relations)
    logging.info(f"Récupération de entity_transactions pour {database_name}")
    try:
        crm_fields = data_fetcher.fetch_crm_fields("deal")  # Fetch crm fields
        deal_data_list = data_fetcher.fetch_data_by_entity("deal", crm_fields)  # Fetch deal data using crm fields
        df = data_processor.process_product_data(deal_data_list)  # Process deal data (assuming it has similar structure as product data)
        df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
        df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
        database_handler.ping_database()
        database_handler.insert_data(df, [], 'deal', data_fetcher.total_entries)  # Insert deal data, assuming no date columns
        data_fetcher.total_entries = None
        logging.info(f"Traitement réussi des transactions pour {database_name}")
    except Exception as e:
        logging.error(f'Erreur lors de la récupération des données de transaction pour {database_name}: {str(e)}')



    #Entity_Contacts (Table avec toutes les relations)
    logging.info(f"Récupération de entity_contacts pour {database_name}")
    try:
        database_handler.ping_database()
        contact_fields = data_fetcher.fetch_crm_fields("contact")  # Fetch contact fields
        contact_data_list = data_fetcher.fetch_data_by_entity("contact", contact_fields)  # Fetch contact data using contact fields
        df = data_processor.process_product_data(contact_data_list)  # Process contact data (assuming it has similar structure as product data)
        df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
        df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
        database_handler.ping_database()
        database_handler.insert_data(df, [], 'contact', data_fetcher.total_entries)  # Insert contact data, assuming no date columns
        data_fetcher.total_entries = None
        logging.info(f"Traitement réussi des contacts pour {database_name}")
    except Exception as e:
        logging.error(f'Erreur lors de la récupération des données de contact pour {database_name}: {str(e)}')



    # Entity_Prospect (Table avec toutes les relations)
    logging.info(f"Récupération de entity_prospects pour {database_name}")
    try:
        crm_fields = data_fetcher.fetch_crm_fields("lead")  # Fetch crm fields
        deal_data_list = data_fetcher.fetch_data_by_entity("lead", crm_fields)  # Fetch deal data using crm fields
        df = data_processor.process_product_data(deal_data_list)  # Process deal data (assuming it has similar structure as product data)
        df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
        df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
        database_handler.ping_database()
        database_handler.insert_data(df, [], 'lead', data_fetcher.total_entries)  # Insert deal data, assuming no date columns
        data_fetcher.total_entries = None
        logging.info(f"Traitement réussi des prospects pour {database_name}")
    except Exception as e:
        logging.error(f'Erreur lors de la récupération des données de prospect pour {database_name}: {str(e)}')

    # Entity_Companys (Table avec toutes les relations)
    logging.info(f"Récupération de entity_compagnies pour {database_name}")
    try:
        company_fields = data_fetcher.fetch_crm_fields("company")  # Fetch company fields
        company_data_list = data_fetcher.fetch_data_by_entity("company", company_fields)  # Fetch company data using company fields
        df = data_processor.process_product_data(company_data_list)  # Process company data (assuming it has similar structure as product data)
        df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
        df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
        database_handler.ping_database()
        database_handler.insert_data(df, [], 'company', data_fetcher.total_entries)  # Insert company data, assuming no date columns
        data_fetcher.total_entries = None
        logging.info(f"Traitement réussi des compagnies pour {database_name}")
    except Exception as e:
        logging.error(f'Erreur lors de la récupération des données de la compagnie pour {database_name}: {str(e)}')

    # Transaction et Contacts et Lead Angers
    if database_name == 'bitrix_angers':
        logging.info(f"Récupération des table crm... et crm_..._uf pour Prospects / Transactions / Contacts pour {database_name}")
        try:
            list_names = ['crm_lead', 'crm_lead_uf', 'crm_deal', 'crm_deal_uf', 'crm_contact', 'crm_contact_uf', 'crm_company', 'crm_company_uf']
            primary_keys = {'crm_lead': 'ID', 'crm_lead_uf': 'LEAD_ID', 'crm_deal': 'ID', 'crm_deal_uf': 'DEAL_ID', 'crm_contact': 'ID', 'crm_contact_uf': 'CONTACT_ID', 'crm_company': 'ID', 'crm_company_uf': 'COMPANY_ID'}
            for list_name in list_names:
                logging.info(f"Traitement de {list_name} pour {database_name}")
                df = data_fetcher.fetch_and_create_dataframe(list_name, database_name)
                df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
                df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
                num_rows = len(df)
                logging.info(f"{num_rows} lignes récupérées pour {list_name} pour {database_name}")

                # Exclusion des colonnes spécifiques pour crm_deal_uf
                if list_name == 'crm_deal_uf':
                    original_columns = set(df.columns)
                    df = df.drop(columns=colonnes_a_exclure_de_crm_deal_uf, errors='ignore')
                    removed_columns = original_columns - set(df.columns)
                    logging.info(f"Colonnes supprimées pour {list_name}: {', '.join(removed_columns)}")

                database_handler.ping_database()
                database_handler.dynamic_table_insert(df, list_name, primary_key=primary_keys.get(list_name, 'ID'))  # Utiliser 'ID' par défaut si la clé primaire n'est pas spécifiée
                logging.info(f"Traitement réussi de la liste {list_name} pour {database_name}")
        except Exception as e:
            logging.error(f"Erreur lors de l'actualisation de {list_name} pour {database_name}: {str(e)}")


    # Transaction et Contacts et Lead Nantes
    if database_name == 'bitrix_nantes':
        logging.info(f"Récupération des table crm... et crm_..._uf pour Prospects / Transactions / Contacts pour {database_name}")
        try:
            list_names = ['crm_lead', 'crm_lead_uf', 'crm_deal', 'crm_deal_uf', 'crm_contact', 'crm_contact_uf', 'crm_company', 'crm_company_uf']
            primary_keys = {'crm_lead': 'ID', 'crm_lead_uf': 'LEAD_ID', 'crm_deal': 'ID', 'crm_deal_uf': 'DEAL_ID', 'crm_contact': 'ID', 'crm_contact_uf': 'CONTACT_ID', 'crm_company': 'ID', 'crm_company_uf': 'COMPANY_ID'}
            for list_name in list_names:
                logging.info(f"Traitement de {list_name} pour {database_name}")
                df = data_fetcher.fetch_and_create_dataframe(list_name, database_name)
                df = DataProcessor.clean_specific_values(df)  # Nettoyer les valeurs spécifiques
                df = DataProcessor.replace_empty_with_null(df)  # Remplacer les valeurs vides par NULL
                num_rows = len(df)
                logging.info(f"{num_rows} lignes récupérées pour {list_name} pour {database_name}")

                database_handler.ping_database()
                database_handler.dynamic_table_insert(df, list_name, primary_key=primary_keys.get(list_name, 'ID'))  # Utiliser 'ID' par défaut si la clé primaire n'est pas spécifiée
                logging.info(f"Traitement réussi de la liste {list_name} pour {database_name}")
        except Exception as e:
            logging.error(f"Erreur lors de l'actualisation de {list_name} pour {database_name}: {str(e)}")

logging.info(f"Fin du scrypt d'extraction / transformation / chargement")
