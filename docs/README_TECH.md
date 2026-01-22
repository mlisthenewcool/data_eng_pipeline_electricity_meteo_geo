Choix du synchrone (httpx + HTTP/2) L’usage de httpx en mode synchrone est privilégié, 
car Airflow orchestre déjà le parallélisme au niveau des tâches, rendant l'asynchrone 
inutile. Cette approche évite les conflits avec la boucle d'événements du scheduler, 
simplifie la gestion des erreurs et reste parfaitement adaptée aux traitements de 
données qui sont majoritairement limités par le CPU.

Gestion d'état hybride (JSON + Airflow DB) L'utilisation de fichiers JSON locaux 
permet de stocker des métadonnées riches (ETags, hashes SHA256) indispensables à une 
logique de "smart skip" que la base de données d'Airflow ne peut pas gérer nativement. 
Ce système hybride contourne les limites de taille des XComs tout en offrant une 
meilleure portabilité et une inspection manuelle simplifiée des états du pipeline.