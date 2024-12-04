import socket
import threading
import os
import time
import struct
import json

# CONSTANTES GLOBALES
PORT_PRINCIPAL = 3463
PORT_SECONDAIRE = PORT_PRINCIPAL + 1
MAX_TENTATIVES = 10

# NOM DE LA MACHINE (WORKER)
NOM_MACHINE = socket.gethostname()

# Dictionnaire global pour stocker les occurrences de mots
occurrences_mots = {}


###################################################
# FONCTIONS D'ENVOI/RECEPTION DE MESSAGES
###################################################

def envoyer_message(socket_connexion, message, silencieux=False):
    """
    Envoie un message via un socket TCP.
    
    Args:
        socket_connexion (socket.socket): Le socket utilisé pour l'envoi.
        message (str): Le message à envoyer au format texte.
        silencieux (bool): Si True, aucune information n'est affichée sur la console.
                          Sinon, un message de confirmation est affiché.
    """
    try:
        message_bytes = message.encode('utf-8')
        taille_message = struct.pack('!I', len(message_bytes))
        socket_connexion.sendall(taille_message + message_bytes)
        if not silencieux:
            print(f"'{NOM_MACHINE}' : Message envoyé : {message}")
    except Exception as e:
        if not silencieux:
            print(f"'{NOM_MACHINE}' : Erreur lors de l'envoi du message : {e}")


def recevoir_message(socket_connexion, silencieux=False):
    """
    Reçoit un message via un socket TCP.
    
    Args:
        socket_connexion (socket.socket): Le socket utilisé pour la réception.
        silencieux (bool): Si True, aucune information n'est affichée.
                           Sinon, le message reçu est affiché.

    Returns:
        str ou None: Le message reçu en texte. Retourne None si la connexion est fermée.
    """
    try:
        taille_message_bytes = b''
        while len(taille_message_bytes) < 4:
            packet = socket_connexion.recv(4 - len(taille_message_bytes))
            if not packet:
                return None
            taille_message_bytes += packet

        taille_message = struct.unpack('!I', taille_message_bytes)[0]

        message_bytes = b''
        while len(message_bytes) < taille_message:
            packet = socket_connexion.recv(taille_message - len(message_bytes))
            if not packet:
                return None
            message_bytes += packet

        message = message_bytes.decode('utf-8')
        if not silencieux:
            print(f"'{NOM_MACHINE}' : Message reçu : {message}")
        return message

    except Exception as e:
        if not silencieux:
            print(f"'{NOM_MACHINE}' : Erreur lors de la réception du message : {e}")
        return None


###################################################
# FONCTIONS POUR LA CONNEXION AU MASTER
###################################################

def fermer_connexion_master(socket_master):
    """
    Ferme la connexion avec le master.
    
    Args:
        socket_master (socket.socket): Le socket correspondant à la connexion avec le master.
    """
    try:
        socket_master.close()
        print(f"'{NOM_MACHINE}' : Connexion avec le master fermée.")
    except Exception as e:
        print(f"'{NOM_MACHINE}' : Erreur lors de la fermeture de la connexion avec le master : {e}")


def connexion_au_master():
    """
    Prépare et met en écoute un socket pour établir la connexion avec le master 
    sur le port principal (PORT_PRINCIPAL).
    Gère également les cas où le port est déjà utilisé (tentatives multiples).
    
    Returns:
        socket.socket: Le socket lié et mis en écoute pour la connexion du master.
    """
    socket_master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for tentative in range(MAX_TENTATIVES):
        try:
            socket_master.bind(('0.0.0.0', PORT_PRINCIPAL))
            print(f"'{NOM_MACHINE}' : Le socket est lié au port {PORT_PRINCIPAL} après {tentative + 1} tentative(s).")
            break
        except OSError:
            if tentative < MAX_TENTATIVES - 1:
                print(f"'{NOM_MACHINE}' : Le port {PORT_PRINCIPAL} est déjà utilisé. "
                      f"Tentative de libération du port ({tentative + 1}/{MAX_TENTATIVES})...")
                pid = os.popen(f'lsof -t -i:{PORT_PRINCIPAL}').read().strip()
                print(f"'{NOM_MACHINE}' : PID du processus utilisant le port {PORT_PRINCIPAL} : {pid}")
                if pid:
                    os.system(f'kill -9 {pid}')
                    print(f"'{NOM_MACHINE}' : Tentative de tuer le processus {pid}.")
                else:
                    print(f"'{NOM_MACHINE}' : Aucun processus n'utilise le port {PORT_PRINCIPAL}.")
                time.sleep(5)
            else:
                raise Exception(f"'{NOM_MACHINE}' : Impossible de lier le socket au port {PORT_PRINCIPAL} "
                                f"après {MAX_TENTATIVES} tentatives.")

    socket_master.listen(5)
    print(f"'{NOM_MACHINE}' : PHASE CONNEXION 1 : Le worker écoute sur le port {PORT_PRINCIPAL} "
          "pour les connexions du master.")
    return socket_master


##########################################################
# FONCTIONS POUR LA CONNEXION AUX AUTRES WORKERS
##########################################################

def connexion_aux_workers():
    """
    Prépare et met en écoute un socket pour se connecter aux autres workers 
    sur le port secondaire (PORT_SECONDAIRE).
    Gère les tentatives multiples si le port est occupé.
    
    Returns:
        socket.socket: Le socket lié et mis en écoute pour les connexions des autres workers.
    """
    socket_workers = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for tentative in range(MAX_TENTATIVES):
        try:
            socket_workers.bind(('0.0.0.0', PORT_SECONDAIRE))
            print(f"'{NOM_MACHINE}' : Le socket est lié au port {PORT_SECONDAIRE} "
                  f"après {tentative + 1} tentative(s).")
            break
        except OSError:
            if tentative < MAX_TENTATIVES - 1:
                print(f"'{NOM_MACHINE}' : Le port {PORT_SECONDAIRE} est déjà utilisé. "
                      f"Tentative de libération du port ({tentative + 1}/{MAX_TENTATIVES})...")
                pid = os.popen(f'lsof -t -i:{PORT_SECONDAIRE}').read().strip()
                print(f"'{NOM_MACHINE}' : PID du processus utilisant le port {PORT_SECONDAIRE} : {pid}")
                if pid:
                    os.system(f'kill -9 {pid}')
                    print(f"'{NOM_MACHINE}' : Tentative de tuer le processus {pid}.")
                else:
                    print(f"'{NOM_MACHINE}' : Aucun processus n'utilise le port {PORT_SECONDAIRE}.")
                time.sleep(5)
            else:
                raise Exception(f"'{NOM_MACHINE}' : Impossible de lier le socket au port {PORT_SECONDAIRE} "
                                f"après {MAX_TENTATIVES} tentatives.")

    socket_workers.listen(5)
    print(f"'{NOM_MACHINE}' : PHASE CONNEXION 2 : Le worker écoute sur le port {PORT_SECONDAIRE} "
          "pour les connexions des autres workers.")
    return socket_workers


def recevoir_msg_workers(socket_worker_connexion, worker_address):
    """
    Gère la communication entrante d'un autre worker.
    Lit les messages reçus en boucle et met à jour les occurrences de mots.
    
    Args:
        socket_worker_connexion (socket.socket): Le socket de connexion avec l'autre worker.
        worker_address (tuple): L'adresse (host, port) du worker distant.
    """
    while True:
        try:
            message = recevoir_message(socket_worker_connexion, silencieux=True)
            if message is None:
                # Connexion fermée par le worker distant
                break
            print(f"'{NOM_MACHINE}' : Message reçu de {worker_address} : {message}")
            compter_occurrences(message)
        except ConnectionError:
            break
        except Exception as e:
            print(f"'{NOM_MACHINE}' : Erreur lors de la communication avec {worker_address} : {e}")
            break


def accepter_connexions_workers(socket_workers, connexions_workers):
    """
    Accepte en boucle les connexions entrantes des autres workers 
    et démarre un thread dédié à chacun.
    
    Args:
        socket_workers (socket.socket): Le socket en écoute sur PORT_SECONDAIRE.
        connexions_workers (dict): Dictionnaire où seront stockées les connexions 
                                   {adresse_worker: socket}.
    """
    while True:
        try:
            socket_worker_connexion, worker_address = socket_workers.accept()
            print(f"'{NOM_MACHINE}' : Connexion acceptée d'un worker : {worker_address}")
            connexions_workers[worker_address] = socket_worker_connexion
            thread_comm = threading.Thread(target=recevoir_msg_workers,
                                           args=(socket_worker_connexion, worker_address))
            thread_comm.start()
        except Exception as e:
            print(f"'{NOM_MACHINE}' : Erreur lors de l'acceptation d'une connexion worker : {e}")
            break


def fermer_connexions_workers(connexions_workers):
    """
    Ferme toutes les connexions établies avec les autres workers.
    
    Args:
        connexions_workers (dict): Dictionnaire {adresse_worker: socket}.
    """
    for addr, sock in connexions_workers.items():
        try:
            sock.close()
            print(f"'{NOM_MACHINE}' : Connexion fermée avec le worker {addr}")
        except Exception as e:
            print(f"'{NOM_MACHINE}' : Erreur lors de la fermeture de la connexion avec {addr} : {e}")


##########################################################
# FONCTIONS POUR LA COMMUNICATION AVEC LES AUTRES WORKERS
##########################################################

def compter_occurrences(message):
    """
    Met à jour le dictionnaire global occurrences_mots avec les occurrences
    des mots trouvés dans le message.
    
    Args:
        message (str): Le message contenant des mots séparés par des espaces.
    """
    global occurrences_mots
    mots = message.split()
    for mot in mots:
        occurrences_mots[mot] = occurrences_mots.get(mot, 0) + 1


def connexion_aux_autres_workers(machines_reçues):
    """
    Etablit des connexions vers les autres workers (pour la phase MAP/SHUFFLE).
    
    Args:
        machines_reçues (list): Liste des noms/addresses des machines workers.

    Returns:
        dict: Dictionnaire {nom_machine_worker: socket} pour chaque worker connecté.
    """
    connexions_workers = {}
    for machine in machines_reçues:
        if machine != NOM_MACHINE:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((machine, PORT_SECONDAIRE))
                connexions_workers[machine] = sock
                print(f"'{NOM_MACHINE}' : Connexion établie avec le worker {machine}")
            except Exception as e:
                print(f"'{NOM_MACHINE}' : Erreur lors de la connexion au worker {machine}: {e}")
    return connexions_workers


def gerer_communication_entre_workers(connexions_workers, list_mots, machines_reçues):
    """
    Distribue les mots entre les workers. Le mot est soit traité localement,
    soit envoyé au worker désigné par la logique (longueur du mot % nb de machines).
    
    Args:
        connexions_workers (dict): Connexions aux autres workers {nom_machine_worker: socket}.
        list_mots (list): Liste de mots à distribuer.
        machines_reçues (list): Liste des machines workers, incluant NOM_MACHINE.
    """
    def envoyer_mots():
        for mot in list_mots:
            machine_number = len(mot) % len(machines_reçues)
            machine_cible = machines_reçues[machine_number]

            if machine_cible == NOM_MACHINE:
                # Traiter localement
                compter_occurrences(mot)
                print(f"'{NOM_MACHINE}' : Mot '{mot}' traité localement")
            else:
                envoyer_message(connexions_workers[machine_cible], mot, silencieux=True)
                print(f"'{NOM_MACHINE}' : Mot '{mot}' envoyé à la machine {machine_cible}")

    thread_envoi = threading.Thread(target=envoyer_mots)
    thread_envoi.start()
    thread_envoi.join()


def sauvegarder_occurrences():
    """
    Sauvegarde le dictionnaire occurrences_mots dans un fichier JSON.
    Le fichier est nommé "{NOM_MACHINE}_results.json" et placé dans le répertoire courant.
    
    Returns:
        str ou None: Le chemin complet du fichier sauvegardé ou None en cas d'erreur.
    """
    fichier = os.path.join(os.getcwd(), f"{NOM_MACHINE}_results.json")
    try:
        with open(fichier, "w", encoding="utf-8") as f:
            json.dump(occurrences_mots, f, ensure_ascii=False, indent=4)
        print(f"'{NOM_MACHINE}' : Dictionnaire des occurrences sauvegardé dans {fichier}")
        return fichier
    except Exception as e:
        print(f"'{NOM_MACHINE}' : Erreur lors de la sauvegarde du dictionnaire : {e}")
        return None


#############################################################
# FONCTION PRINCIPALE POUR LA COMMUNICATION AVEC LE MASTER
#############################################################

def gerer_communication_avec_master(socket_master):
    """
    Gère la communication avec le master.
    Répond aux messages du master et exécute les étapes du MapReduce :
    - Réception de la liste des machines
    - Réception du SPLIT
    - Phase MAP/SHUFFLE
    - Phase SAVE
    - Envoie "CONNEXION OK", "RECEPTION MACHINES OK", "RECEPTION SPLIT OK", etc.
    
    Args:
        socket_master (socket.socket): Le socket de connexion avec le master.
    """
    connexions_workers = None
    list_mots = None
    machines_reçues = None
    msg_split = None

    while True:
        msg_recu = recevoir_message(socket_master)

        if msg_recu is None:
            # Le master a fermé la connexion
            break

        if msg_recu.startswith("MACHINES : "):
            msg_machine = msg_recu[11:]
            machines_reçues = json.loads(msg_machine)
            envoyer_message(socket_master, "RECEPTION MACHINES OK")

        if msg_recu.startswith("SPLIT : "):
            msg_split = msg_recu[8:]
            envoyer_message(socket_master, "RECEPTION SPLIT OK")

        if msg_recu == "GO MAP SHUFFLE":
            connexions_workers = connexion_aux_autres_workers(machines_reçues)
            if connexions_workers:
                envoyer_message(socket_master, "CONNEXION WORKERS OK")
            else:
                envoyer_message(socket_master, "CONNEXION WORKERS FAILED")

        if msg_recu == "START MAP SHUFFLE":
            if msg_split:
                list_mots = msg_split.split()
                gerer_communication_entre_workers(connexions_workers, list_mots, machines_reçues)
            envoyer_message(socket_master, "END MAP SHUFFLE")

        if msg_recu == "SAVE":
            fichier_sauvegarde = sauvegarder_occurrences()
            if fichier_sauvegarde:
                envoyer_message(socket_master, f"SAVE OK : {fichier_sauvegarde}")

        if msg_recu == "END":
            if connexions_workers:
                fermer_connexions_workers(connexions_workers)
            fermer_connexion_master(socket_master)
            break


###################################################
# SCRIPT PRINCIPAL
###################################################

# Connexion au master
socket_master = connexion_au_master()
socket_master_connexion, master_address = socket_master.accept()
print(f"'{NOM_MACHINE}' : Connexion acceptée du master : {master_address}")

# Connexion aux autres workers
socket_workers = connexion_aux_workers()
connexions_workers = {}
thread_accept = threading.Thread(target=accepter_connexions_workers,
                                 args=(socket_workers, connexions_workers))
thread_accept.start()

# Une fois prêt, envoi de "CONNEXION OK" au master
envoyer_message(socket_master_connexion, "CONNEXION OK")

# Gérer la communication avec le master
gerer_communication_avec_master(socket_master_connexion)

# Attendre la fin de l'acceptation des connexions des autres workers
thread_accept.join()

print(f"'{NOM_MACHINE}' : END OF THE SCRIPT")
