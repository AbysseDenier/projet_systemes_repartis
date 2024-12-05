import json
import re
import os
import time

# CONSTANTES GLOBALES
FICHIER_MESSAGE = "input_message.txt"
FICHIER_RESULTATS = "final_aggregated_results.json"
FICHIER_RESULTATS_AMDAHL = "resultats_amdahl.json"
NOMBRE_MACHINES = 1  # Nombre de machines utilisées pour le calcul parallèle (seulement le master, car pas de calculs parallèles)

# Mesure du temps de début
start_time = time.perf_counter()

# Lecture du message 
with open(FICHIER_MESSAGE, "r", encoding="utf-8") as f:
    grand_message = f.read()

###################################################
# FONCTION DE SPLITTING ET NETTOYAGE DU TEXTE
###################################################

def nettoyer_et_decouper_message(big_msg):
    """
    Nettoie le message et le découpe en mots individuels.
    Gère la normalisation du texte, la suppression de certains caractères
    et la séparation des contractions françaises.
    
    Args:
        big_msg (str): Le message complet à traiter.
        
    Returns:
        list: Liste des mots nettoyés.
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

    return final_words

# Nettoyage et découpage du message
tous_les_mots = nettoyer_et_decouper_message(grand_message)

###################################################
# COMPTAGE DES MOTS
###################################################

compte_mots = {}
for mot in tous_les_mots:
    if mot in compte_mots:
        compte_mots[mot] += 1
    else:
        compte_mots[mot] = 1

# Tri par ordre décroissant
sorted_results = dict(sorted(compte_mots.items(), key=lambda x: x[1], reverse=True))

# Sauvegarde du fichier final agrégé
try:
    with open(FICHIER_RESULTATS, "w", encoding="utf-8") as f:
        json.dump(sorted_results, f, ensure_ascii=False, indent=4)
    print(f"[Master] Fichier de résultats final enregistré dans {os.path.abspath(FICHIER_RESULTATS)}")
except Exception as e:
    print(f"[Master] Erreur lors de l'écriture du fichier final : {e}")

# Mesure du temps de fin
end_time = time.perf_counter()

# Calcul et affichage du temps écoulé
elapsed_time = end_time - start_time
print(f"[Master] Temps d'exécution du script : {elapsed_time:.4f} secondes")

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
