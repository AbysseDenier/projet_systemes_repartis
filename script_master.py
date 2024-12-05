import socket
import json
import threading
import struct
import re
import os
import time

# CONSTANTES GLOBALES
PORT_PRINCIPAL = 3463
FICHIER_MACHINES = "machines.txt"
FICHIER_MESSAGE = "input_message.txt"
FICHIER_RESULTATS = "final_aggregated_results.json"
FICHIER_RESULTATS_AMDAHL = "resultats_amdahl.json"


# Lecture du message à envoyer depuis un fichier texte
with open(FICHIER_MESSAGE, "r", encoding="utf-8") as f:
    GRAND_MESSAGE = f.read()


###################################################
# FONCTION DE SPLITTING
###################################################

def decouper_message(big_msg, nb_machine):
    """
    Découpe le message en parties égales (approximativement) entre les workers.
    Gère la normalisation du texte, la suppression de certains caractères
    et la séparation des contractions françaises.
    
    Args:
        big_msg (str): Le message complet à traiter.
        nb_machine (int): Le nombre de machines workers.
        
    Returns:
        list: Liste des segments du message pour chaque worker.
    """
    big_msg_clean = big_msg.lower()
    big_msg_clean = big_msg_clean.replace('’', "'")

    # Caractères autorisés : lettres, chiffres, apostrophe, espace, accents
    big_msg_clean = re.sub(r"[^a-z0-9'\sàâäéèêëïîôöùûüç]", '', big_msg_clean)
    mots = [w for w in big_msg_clean.split() if w]

    # Contractions
    pattern_contraction = re.compile(r"^(l|j|c|d|m|t|s|n|qu)'(.+)$", re.IGNORECASE)
    final_words = []
    for w in mots:
        m = pattern_contraction.match(w)
        if m:
            final_words.append(m.group(1) + "'")
            if m.group(2):
                final_words.append(m.group(2))
        else:
            final_words.append(w)

    longueur = len(final_words)
    nb_mots_par_machine = longueur // nb_machine
    messages_specifiques = []
    cnt = 0
    for i in range(nb_machine):
        if i == nb_machine - 1:
            sub_str = ' '.join(final_words[cnt:])
        else:
            sub_str = ' '.join(final_words[cnt:cnt + nb_mots_par_machine])
        messages_specifiques.append(sub_str)
        cnt += nb_mots_par_machine

    for i, part in enumerate(messages_specifiques):
        print(f"[Master] Partie {i + 1} du message envoyée aux workers : {part}")

    return messages_specifiques


###################################################
# FONCTIONS POUR ENVOI/RECEPTION DE MESSAGES
###################################################

def envoyer_message(socket_client, message, nom_machine_worker):
    """
    Envoie un message via le socket fourni.
    
    Args:
        socket_client (socket.socket): Le socket du worker cible.
        message (str): Le message à envoyer.
        nom_machine_worker (str): Le nom (ou adresse) du worker.
    """
    try:
        message_bytes = message.encode('utf-8')
        taille_message = struct.pack('!I', len(message_bytes))
        socket_client.sendall(taille_message + message_bytes)
        print(f"[Master] Message envoyé à {nom_machine_worker} : {message}")
    except Exception as e:
        print(f"[Master] Erreur lors de l'envoi du message à {nom_machine_worker} : {e}")


def recevoir_message(socket_client, nom_machine_worker):
    """
    Reçoit un message via le socket fourni.
    
    Args:
        socket_client (socket.socket): Le socket du worker source.
        nom_machine_worker (str): Le nom (ou adresse) du worker source.
        
    Returns:
        str: Le message reçu.
    """
    try:
        taille_message_bytes = b''
        while len(taille_message_bytes) < 4:
            packet = socket_client.recv(4 - len(taille_message_bytes))
            if not packet:
                raise ConnectionError("Connexion fermée par le client")
            taille_message_bytes += packet

        taille_message = struct.unpack('!I', taille_message_bytes)[0]

        message_bytes = b''
        while len(message_bytes) < taille_message:
            packet = socket_client.recv(taille_message - len(message_bytes))
            if not packet:
                raise ConnectionError("Connexion fermée par le client")
            message_bytes += packet

        message = message_bytes.decode('utf-8')
        print(f"[Master] Message reçu de {nom_machine_worker} : {message}")
        return message

    except Exception as e:
        print(f"[Master] Erreur lors de la réception du message de {nom_machine_worker} : {e}")
        raise


def envoyer_message_a_tous(connexions, message):
    """
    Envoie un message identique à tous les workers.
    
    Args:
        connexions (dict): Dictionnaire {nom_machine_worker: socket}.
        message (str): Le message à envoyer.
    """
    for machine, socket_client in connexions.items():
        envoyer_message(socket_client, message, machine)


def envoyer_messages_specifiques(connexions, messages_specifiques):
    """
    Envoie des messages spécifiques à chaque worker.
    
    Args:
        connexions (dict): Dictionnaire {nom_machine_worker: socket}.
        messages_specifiques (list): Liste des messages dans l'ordre des workers.
    """
    for (machine, socket_client), msg in zip(connexions.items(), messages_specifiques):
        envoyer_message(socket_client, msg, machine)



########################################################
# FONCTIONS POUR GERER LA CONNEXION AVEC LES WORKERS
########################################################

def connexion_aux_workers(machines):
    """
    Etablit la connexion avec chaque worker.
    
    Args:
        machines (list): Liste des adresses des machines workers.
        
    Returns:
        dict: Dictionnaire {nom_machine_worker: socket}.
    """
    connexions = {}
    for machine in machines:
        try:
            socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_client.connect((machine, PORT_PRINCIPAL))
            connexions[machine] = socket_client
            print(f"[Master] Connexion établie avec le worker {machine}")
        except Exception as e:
            print(f"[Master] Erreur lors de la connexion au worker {machine} : {e}")
    return connexions


def fermer_connexions_workers(connexions):
    """
    Ferme les connexions avec tous les workers.
    
    Args:
        connexions (dict): Dictionnaire {nom_machine_worker: socket}.
    """
    for machine, socket_client in connexions.items():
        try:
            socket_client.close()
            print(f"[Master] Connexion fermée avec le worker {machine}")
        except Exception as e:
            print(f"[Master] Erreur lors de la fermeture de la connexion avec {machine} : {e}")



###################################################
# FONCTION PRINCIPALE POUR DIRIGER LES WORKERS
###################################################

def gerer_communication_avec_workers(connexions, machines_json, results_data):
    """
    Gère toute la communication avec les workers :
    - Connexion initiale
    - Envoi des machines
    - Envoi du SPLIT
    - Phase MAP SHUFFLE
    - Phase SAVE
    - Récupération des chemins de sauvegarde
    
    Met à jour results_data avec les chemins de sauvegarde.
    
    Args:
        connexions (dict): Dictionnaire {nom_machine_worker: socket} contenant les connexions aux workers.
        machines_json (str): Chaîne JSON représentant la liste des machines workers.
        results_data (dict): Dictionnaire pour stocker les chemins de sauvegarde des résultats des workers.
    """
    workers_connectes = {m: False for m in connexions.keys()}
    workers_machines_reception = {m: False for m in connexions.keys()}
    workers_split_reception = {m: False for m in connexions.keys()}
    workers_connexion_workers_ok = {m: False for m in connexions.keys()}
    workers_map_shuffle_reception = {m: False for m in connexions.keys()}
    workers_save_ok = {m: False for m in connexions.keys()}

    workers_save_paths = {}

    split_envoye = False
    machines_envoyees = False
    map_shuffle_envoye = False
    start_map_shuffle_envoye = False
    save_envoye = False

    nb_machine = len(connexions)
    print(f"[Master] Nombre de machines connectées : {nb_machine}")

    parties_message = decouper_message(GRAND_MESSAGE, nb_machine)

    while True:
        for machine, socket_client in connexions.items():
            try:
                message = recevoir_message(socket_client, machine)

                # CONNEXIONS
                #---------------------------------
                if message == "CONNEXION OK":
                    workers_connectes[machine] = True
                    print(f"[Master] Connexions : {workers_connectes}")

                if all(workers_connectes.values()) and not machines_envoyees:
                    envoyer_message_a_tous(connexions, f"MACHINES : {machines_json}")
                    machines_envoyees = True

                if message == "RECEPTION MACHINES OK":
                    workers_machines_reception[machine] = True
                    print(f"[Master] Réceptions machines : {workers_machines_reception}")

                # SPLIT
                #---------------------------------
                if all(workers_machines_reception.values()) and not split_envoye:
                    print("[Master] Envoi du SPLIT à chaque worker.")
                    messages_a_envoyer = [f"SPLIT : {part}" for part in parties_message]
                    envoyer_messages_specifiques(connexions, messages_a_envoyer)
                    split_envoye = True

                if message == "RECEPTION SPLIT OK":
                    workers_split_reception[machine] = True
                    print(f"[Master] Réceptions SPLIT : {workers_split_reception}")

                # MAP SHUFFLE
                #---------------------------------
                if all(workers_split_reception.values()) and not map_shuffle_envoye:
                    envoyer_message_a_tous(connexions, "GO MAP SHUFFLE")
                    map_shuffle_envoye = True

                if message in ("CONNEXION WORKERS OK", "CONNEXION WORKERS FAILED"):
                    workers_connexion_workers_ok[machine] = True
                    print(f"[Master] Connexions entre workers : {workers_connexion_workers_ok}")

                if all(workers_connexion_workers_ok.values()) and not start_map_shuffle_envoye:
                    envoyer_message_a_tous(connexions, "START MAP SHUFFLE")
                    start_map_shuffle_envoye = True

                if message == "END MAP SHUFFLE":
                    workers_map_shuffle_reception[machine] = True
                    print(f"[Master] Réceptions END MAP SHUFFLE : {workers_map_shuffle_reception}")

                # SAVE
                #---------------------------------
                if all(workers_map_shuffle_reception.values()) and not save_envoye:
                    envoyer_message_a_tous(connexions, "SAVE")
                    save_envoye = True

                if message.startswith("SAVE OK : "):
                    chemin_fichier = message[10:].strip()
                    workers_save_ok[machine] = True
                    workers_save_paths[machine] = chemin_fichier
                    print(f"[Master] {machine} a sauvegardé : {chemin_fichier}")
                    print(f"[Master] Confirmations SAVE : {workers_save_ok}")

                # END
                #---------------------------------
                if all(workers_save_ok.values()) and save_envoye:
                    print("[Master] Tous les workers ont sauvegardé leurs fichiers :")
                    for wkr, path in workers_save_paths.items():
                        print(f"  - {wkr} : {path}")
                    results_data['workers_save_paths'] = workers_save_paths

                    envoyer_message_a_tous(connexions, "END")
                    return

            except Exception as e:
                print(f"[Master] Erreur lors de la réception depuis {machine} : {e}")




###################################################
# SCRIPT PRINCIPAL
###################################################

# Mesure du temps de début pour la communication avec les workers
start_time = time.perf_counter()

# Lecture du fichier machines.txt pour obtenir la liste des workers
with open(FICHIER_MACHINES, 'r') as file:
    liste_machines = [line.strip() for line in file.readlines()]

# Nombre de machines
NOMBRE_MACHINES = len(liste_machines) + 1  # on compte le master
machines_json = json.dumps(liste_machines)
connexions = connexion_aux_workers(liste_machines)

results_data = {}


# Lancement du thread de communication
thread_communication = threading.Thread(
    target=gerer_communication_avec_workers,
    args=(connexions, machines_json, results_data)
)
thread_communication.start()
thread_communication.join()

fermer_connexions_workers(connexions)

workers_save_paths = results_data.get('workers_save_paths', {})


# Agrégation des résultats finaux
final_results = {}
for wkr, path in workers_save_paths.items():
    try:
        with open(path, "r", encoding="utf-8") as f:
            worker_data = json.load(f)
        for mot, compte in worker_data.items():
            if mot in final_results:
                final_results[mot] += compte
            else:
                final_results[mot] = compte
    except Exception as e:
        print(f"[Master] Erreur lors de la lecture du fichier {path} de {wkr} : {e}")

# Tri par ordre décroissant
sorted_results = dict(sorted(final_results.items(), key=lambda x: x[1], reverse=True))


# Sauvegarde du fichier final agrégé
try:
    with open(FICHIER_RESULTATS, "w", encoding="utf-8") as f:
        json.dump(sorted_results, f, ensure_ascii=False, indent=4)
    print(f"[Master] Fichier de résultats final enregistré dans {os.path.abspath(FICHIER_RESULTATS)}")
except Exception as e:
    print(f"[Master] Erreur lors de l'écriture du fichier final : {e}")


# Mesure du temps de fin
end_time = time.perf_counter()

# Temps écoulé
elapsed_time = end_time - start_time
print(f"[Master] Temps d'exécution du script avec {NOMBRE_MACHINES} machines : {elapsed_time:.4f} secondes")

# Sauvegarde des résultats de performance dans resultats_amdahl.json
try:
    resultats_amdahl = {}
    if os.path.exists(FICHIER_RESULTATS_AMDAHL):
        with open(FICHIER_RESULTATS_AMDAHL, "r", encoding="utf-8") as f:
            resultats_amdahl = json.load(f)

    # Mise à jour des résultats
    resultats_amdahl[str(NOMBRE_MACHINES)] = {
        "elapsed_time": elapsed_time
    }

    with open(FICHIER_RESULTATS_AMDAHL, "w", encoding="utf-8") as f:
        json.dump(resultats_amdahl, f, ensure_ascii=False, indent=4)
    print(f"[Master] Résultats de performance enregistrés dans {os.path.abspath(FICHIER_RESULTATS_AMDAHL)}")
except Exception as e:
    print(f"[Master] Erreur lors de l'écriture du fichier de résultats Amdahl : {e}")

print("[Master] Fin du script.")
