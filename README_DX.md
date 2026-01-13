# Développement

## Docker

* Cycle de vie

```shell
# Lancer les services en arrière-plan & reconstruit l'image si modifiée
docker compose up --build --detach

# Arrêter les services
docker compose stop

# Redémarrer les services arrêtés sans recréer les conteneurs
docker compose start

# Supprimer les services (conteneurs & réseaux)
# /!\ attention /!\ ajouter `--volumes` pour supprimer les volumes nommés associés
# ajouter `--rmi` pour supprimer les images associées
docker compose down
```

* Inspection & débug

```shell
# Vérifier l'état des conteneurs
docker compose ps

# Suivre les logs des 50 dernières lignes de chaque service
# ajouter `--follow` pour suivre en temps réel
docker compose logs --tail 50

# Entrer dans le conteneur en ligne de commande pour débugger
docker compose exec airflow_service bash
```

* Maintenance ciblée par service

```shell
docker compose restart airflow_service
docker compose up --build airflow_service
docker compose logs --tail 50 airflow_service
docker compose ps airflow_service
```

* Nettoyage système

```shell
# Supprimer les images "dangling" qui ne sont plus associées
docker image prune
# Nettoyage complet (réseaux, caches de build, images)
docker system prune
# Vérifier l'espace disque consommé par Docker
docker system df 
```