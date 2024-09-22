import os
import json
import shutil
from datetime import datetime

# Fonction pour charger les données depuis un fichier texte (avec encodage UTF-8)
def load_sample(path):
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    return lines

# Générateur pour lire les fichiers ligne par ligne (avec encodage UTF-8)
def load_sample_generator(path):
    with open(path, 'r', encoding='utf-8') as file:
        for line in file:
            yield line

# Fonction pour transformer les données en JSON
def generate_json(lines):
    total_sent = 0
    name = ""
    
    for line in lines:
        # Ignorer les lignes vides ou mal formées
        if not line.strip():
            continue  # Ignore la ligne si elle est vide
        parts = line.strip().split()
        
        # Vérification que la ligne contient bien au moins 3 parties (nom, date, montant)
        if len(parts) < 3:
            print(f"Ligne mal formée ignorée : {line.strip()}")
            continue
        
        name = parts[0]  # On prend le nom de l'émetteur
        
        try:
            # On remplace correctement les caractères spéciaux et on convertit en entier
            montant = int(parts[2].replace('€', '').replace('â‚¬', ''))
            total_sent += montant  # On cumule les montants envoyés
        except ValueError:
            print(f"Montant invalide sur la ligne : {line.strip()}")
            continue  # Ignorer la ligne si le montant est invalide
    
    result = {
        "name": name,
        "total_sent": total_sent
    }
    return result

# Fonction pour sauvegarder le résultat dans un fichier JSON
def save_result(file_path, result):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    result_filename = f'result_{os.path.basename(file_path).replace(".txt", "")}_{timestamp}.json'
    result_path = os.path.join('result', result_filename)
    
    # Sauvegarde du fichier JSON
    with open(result_path, 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, indent=4)
    
    return result_filename

# Fonction pour déplacer les fichiers traités dans le dossier archived
def archive_file(source_path, archived_folder):
    if not os.path.exists(archived_folder):
        os.makedirs(archived_folder)
    shutil.move(source_path, os.path.join(archived_folder, os.path.basename(source_path)))

# Pipeline principale pour traiter un fichier sans générateur
def process_file(file_path, archived_folder):
    print(f"Processing file: {file_path}")
    
    # Chargement des données
    lines = load_sample(file_path)
    
    # Transformation en JSON
    result = generate_json(lines)
    
    # Sauvegarde du résultat
    result_filename = save_result(file_path, result)
    
    # Archiver le fichier traité
    archive_file(file_path, archived_folder)
    
    print(f"File processed and archived: {result_filename}")

# Pipeline principale avec générateur pour éviter la surcharge mémoire
def process_file_with_generator(file_path, archived_folder):
    print(f"Processing file with generator: {file_path}")
    
    # Chargement des données ligne par ligne avec un générateur
    lines_generator = load_sample_generator(file_path)
    
    # Transformation en JSON
    result = generate_json(lines_generator)
    
    # Sauvegarde du résultat
    result_filename = save_result(file_path, result)
    
    # Archiver le fichier traité
    archive_file(file_path, archived_folder)
    
    print(f"File processed and archived: {result_filename}")

# Fonction pour traiter tous les fichiers dans le dossier source
def process_all_files(source_folder, result_folder, archived_folder, use_generator=False):
    # Vérifie si le dossier result existe, sinon le créer
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    # Liste tous les fichiers dans le dossier source
    files = [f for f in os.listdir(source_folder) if f.endswith('.txt')]
    
    if not files:
        print("No files to process.")
        return
    
    for file in files:
        file_path = os.path.join(source_folder, file)
        
        if use_generator:
            process_file_with_generator(file_path, archived_folder)
        else:
            process_file(file_path, archived_folder)

# Exemple d'utilisation
if __name__ == "__main__":
    source_folder = 'source'  # Dossier où se trouvent les fichiers à traiter
    result_folder = 'result'  # Dossier où les résultats seront sauvegardés
    archived_folder = 'archived'  # Dossier où les fichiers traités seront déplacés

    # Pour exécuter sans générateur
    process_all_files(source_folder, result_folder, archived_folder, use_generator=False)

    # Pour exécuter avec générateur (pour les gros fichiers)
    process_all_files(source_folder, result_folder, archived_folder, use_generator=True)
