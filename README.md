# Projet réalisé par : Etienne Eskinazi pour le cours BGD_701 de Monsieur Rémi Sharrock.

# Projet MapReduce en Python pour le cours BGD_701

Ce projet illustre la mise en œuvre d’un système MapReduce distribué en Python, développé dans le cadre du cours de Systèmes Répartis (BGD_701) de Monsieur Rémi Sharrock. L’objectif est de fournir un exemple concret de répartition de tâches de traitement de texte (comptage de mots) entre un **master** et plusieurs **workers**, en simulant une phase de Map/Shuffle/Reduce.

## Description du Projet

Le projet met en place :
- Un **master**, chargé de :
  - Lire un message (depuis un fichier `input_message.txt`).
  - Découper ce message en parties égales (split).
  - Envoyer ces parties à différents workers.
  - Orchestrer les phases du job MapReduce (MAP, SHUFFLE, SAVE).
  - Récupérer les résultats finaux depuis les workers, les agréger et les sauvegarder dans un fichier `final_aggregated_results.json`.

- Des **workers**, chargés de :
  - Recevoir leur partie du texte à traiter (phase MAP).
  - Compter les occurrences de mots.
  - Envoyer/recevoir des mots aux/autres workers si nécessaire (phase SHUFFLE).
  - Sauvegarder leurs résultats individuels.
  - Envoyer au master le chemin du fichier de résultats.

L’ensemble du code utilise des sockets TCP pour la communication, en respectant un protocole simple : le master envoie des commandes et les workers répondent en conséquence. Le code utilise également un découpage du texte basé sur une logique de hachage simple (longueur du mot, etc.) afin de répartir la charge entre les workers.

## Organisation du Code

- **Master** :
  - Lit le message dans `input_message.txt`.
  - Découpe le message en parties et envoie à chaque worker.
  - Lance la phase MAP SHUFFLE.
  - Lance la phase SAVE (demande aux workers de sauvegarder leurs résultats).
  - Agrège tous les fichiers de résultats des workers dans `final_aggregated_results.json`.

- **Workers** :
  - Écoutent sur deux ports : un pour le master, un autre pour les connexions entre workers.
  - Receivent les parties du texte, comptent les occurrences de mots localement.
  - Si nécessaire, envoient certains mots à d’autres workers.
  - Sur demande du master, sauvegardent leurs résultats.
  - Renvoient au master le chemin du fichier sauvegardé.

## Fichiers Principaux

- `machines.txt` : Contient la liste des adresses ou noms des machines workers.
- `input_message.txt` : Le message complet à traiter.
- `final_aggregated_results.json` : Le fichier final d’agrégation des résultats est généré par l'exécution des scripts.
- `pyproject.toml` : Fichier de configuration Poetry pour la gestion des dépendances et de l’environnement du projet.
- `deploy_script.sh` : Fichier bash de lancement des scripts script_worker sur les différents workers.
- Le code Python du master et des workers (script_master et script_worker).

## Pré-requis

- Python 3.10 ou supérieur (jusqu’à 3.12).
- (Poetry pour la gestion des dépendances et de l’environnement virtuel) -> non nécessaire car toutes les fonctionnalités utilisées dans le code (sockets, threading, etc.) font partie de la bibliothèque standard .

Vous pouvez ajuster la version Python dans `pyproject.toml` (par exemple `python = ">=3.10,<3.13"`).

## Installation et Lancement

1. **Cloner le projet** :
   ```bash
   git clone https://github.com/AbysseDenier/projet_systemes_repartis.git
   cd projet_systemes_repartis

2. **Préparer les fichiers** :
    Modifier machines.txt pour lister les workers.
    Placer le texte à traiter dans input_message.txt.

3. **Lancer le script bash** qui exécute le script_worker.py sur chaque worker :
    ```bash
    bash deploy_script.sh

4. **Lancer le master** :
    ```bash
    python3 script_master.py

5. **Exécution du MapReduce** : Une fois tous les workers connectés, le master enverra les étapes successives :
    Envoi des morceaux de texte (SPLIT)
    Lancement du MAP/SHUFFLE
    Demande de sauvegarde (SAVE)
    Récupération des chemins de fichiers résultats
    Agrégation finale (REDUCE)

6. **Résultats** : 
Le résultat final agrégé se trouvera dans final_aggregated_results.json.
