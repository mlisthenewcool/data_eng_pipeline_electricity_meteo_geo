- transformations et jointures à réaliser
    - dataset : ign_contours_iris
        - pas encore sûr de comment traiter le champ geometrie qui est une forme. je dois m'en servir pour lier les
          stations météo les plus proches de mon site de production d'électricité
    - dataset : odre_installations
        - à partir de ce dataset, extraire les sites de production électrique qui peuvent être reliés à la météo
          (photovoltaïque et éolien, autre chose ?)
    - dataset : meteo_france_stations
        - à partir de ce dataset, extraire les stations météo qui peuvent mesurer les champs intéressants pour prédire
          et expliquer la production d'électricité (photovoltaïque et éolien, autre chose ?), leur ajouter un champ
          booléen qui permette de savoir si cette station permet de mesure le photovoltaïque et un autre booléen
          pour l'éolien par exemple

    - à partir de ces trois datasets ci-dessus, joindre les trois pour obtenir les datasets en les liant ainsi :
        - construire la liste des installations de production électrique qui nous intéressent (photovoltaïque et éolien,
          autre chose ?) en ajoutant l'identifiant de la station météo la plus proche qui mesure les bons paramètres
          (photovoltaïque ou éolien par exemple)
        - liste des stations météo (identifiants) pour passer les appels à l'API de Météo France