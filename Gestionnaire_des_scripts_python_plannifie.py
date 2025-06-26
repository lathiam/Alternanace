import subprocess
import time
import os
import logging
import psutil
import schedule
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from Cles_API_et_BDD import DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_CHARSET, DB_NAME1, DB_NAME2

# Configuration de la journalisation
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Handler pour écrire dans un fichier
file_handler = logging.FileHandler('Logs/Logs_Gestionnaire_des_scripts.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
# Handler pour afficher dans la console
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
# Ajouter les deux handlers au logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

print("En attente du lancement à 23:30")


def verifier_scripts(scripts):
    for script in scripts:
        if not os.path.exists(script):
            logging.error(f"Le script {script} n'existe pas.")
            return False
        if not os.access(script, os.X_OK):
            logging.error(f"Le script {script} n'a pas les permissions d'exécution.")
            return False
        logging.info(f"L'accès au script {script} est vérifié avec succès.")
    return True


def chemin_scripts():
    # Chemin de base et scripts
    chemin_base = r"C:\Users\BitrixESPL\EduServices\ESPL Drive - Documents\Equipe Marketing et Communication\Data\10 - Modèles et scripts automatique\Scripts Python"
    chemin_base_2 = r"C:\Users\BitrixESPL\EduServices\ESPL Drive - Documents\Equipe Marketing et Communication\Data\10 - Modèles et scripts automatique\Scripts Python\Transformation"
    scripts_séquentiels = [os.path.join(chemin_base, "ETL_Principal.py"), os.path.join(chemin_base_2, "Nettoyage_et_Modifications_SQL_BDD.py"), os.path.join(chemin_base, "Requetes_SQL_angers.py"), os.path.join(chemin_base, "Requetes_SQL_nantes.py"), os.path.join(chemin_base_2, "rapport_d_erreurs_facturation_replace_csv.py"), os.path.join(chemin_base_2, "ETL_web_scrapping_WordPresse_Angers.py"), os.path.join(chemin_base_2, "Requetes_SQL_web_scrapping.py")]
    scripts_simultanés = []

    # Vérification des scripts séquentiels
    if not verifier_scripts(scripts_séquentiels):
        logging.error("Un ou plusieurs scripts séquentiels sont introuvables ou non exécutables.")
        return

    # Exécution des scripts séquentiels
    for script in scripts_séquentiels:
        logging.info(f"Lancement du script séquentiel {script}")
        lancer_script(script)
        time.sleep(15)  # Attendre 15 secondes avant de lancer le prochain script
        logging.info(f"Fin du script séquentiel {script}")

    # Exécution des scripts simultanés
    processus = []
    for script in scripts_simultanés:
        p = subprocess.Popen(["python", script])
        processus.append(p)

    # Attendre que tous les scripts simultanés se terminent
    for p in processus:
        p.wait()

def lancer_script(script, sequentiel=True):

    debut = time.time()
    try:
        logging.info(f"Début de l'exécution du script {script}")
        # Mesures des ressources avant l'exécution
        time.sleep(1)
        cpu_avant = psutil.cpu_percent(interval=1)
        memoire_avant = psutil.virtual_memory().percent
        logging.info(f"Utilisation CPU avant : {cpu_avant}%, Mémoire avant : {memoire_avant}%")

        subprocess.run(["python", script], check=True)
        #SI BUG ALORS UTILISER LA LIGNE SI DESSOUS POUR AVOIR PLUS DE DETAILS DANS LES LOGS
        #subprocess.run(["python", "-Xfrozen_modules=off", script], check=True)


        # Mesures des ressources après l'exécution
        time.sleep(1)
        cpu_apres = psutil.cpu_percent(interval=1)
        memoire_apres = psutil.virtual_memory().percent
        logging.info(f"Utilisation CPU après : {cpu_apres}%, Mémoire après : {memoire_apres}%")

        fin = time.time()
        logging.info(f"Script {script} exécuté avec succès en {fin - debut} secondes.")
        logging.info(f"Utilisation CPU avant : {cpu_avant}%, après : {cpu_apres}%")
        logging.info(f"Utilisation mémoire avant : {memoire_avant}%, après : {memoire_apres}%")
    except subprocess.CalledProcessError as e:
        logging.error(f"Erreur lors de l'exécution du script {script}: {e}")
        fin = time.time()  # Mettre à jour l'horodatage de fin en cas d'erreur
        logging.info(f"Le script {script} a échoué après {fin - debut} secondes.")



# Planifier l'exécution
schedule.every().day.at("23:30").do(chemin_scripts) #22:30 habituellement


# Boucle infinie pour garder le script en attente
while True:
    schedule.run_pending()
    time.sleep(1)
