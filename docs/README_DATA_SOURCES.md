* librairies pour présenter une carte
    * Kepler.gl, pydeck, lonboard
    * https://github.com/visgl/deck.gl
    * https://github.com/developmentseed/lonboard

* potentially useful libraries
    * inspirations ?
        * https://github.com/CharlieSergeant/airflow-minio-postgres-fastapi/tree/main
        * https://www.electricitymaps.com/data/methodology
        * https://app.electricitymaps.com/coverage?q=fr
    * ETL/ELT: https://github.com/airbytehq/airbyte
    * data git : https://github.com/treeverse/dvc
    * deploy
        * https://github.com/dokploy/dokploy
        * https://github.com/coollabsio/coolify
    * observability
        * https://www.netdata.cloud/
        * grafana (+) prometheus

* Electricity
    * https://odre.opendatasoft.com/explore/dataset/registre-national-installation-production-stockage-electricite-agrege/
        * documentation and exports (parquet is preferred) available on the upper url
        * batch integration: updated once per year but there is no fixed date, the url should always point to the
          latest available data
        * data quality/enhancement
            * check with known datasets for annual production of different scales (sector, region, general)
                * https://analysesetdonnees.rte-france.com/
                * https://www.services-rte.com/fr/visualisez-les-donnees-publiees-par-rte/capacite-installee-de-production.html
    * https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-cons-def
        * todo documentation
        * stratégie de MAJ plus détaillée : https://gemini.google.com/app/804eaee7bb674856
    * https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-tr
        * todo documentation
        * certaines données ne sont pas remontées immédiatement (vérifier, mais environ 1h).

* Météo
    * https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees
        * liste des stations avec historique complet
        * avantages: contient la liste exhaustive des paramètres que mesurent les stations
        * inconvénients: cette liste est publiée avec les noms des paramètres, mais sans les codes associés ce qui
          complique les jointures avec les jeux de données observations / climatologie
    * climatologie (données qualifiées, c'est-à-dire avec un code qualité pour de nombreux paramètres)
        * API asynchrone (commande-fichier)
        * todo: renvoie 196 colonnes tout le temps pour toutes les stations ?
        * API: liste-stations/horaire
            * par département
            * envoie aussi les stations anciennes
            * peut filtrer les stations par type de paramètres mesurés (pression, vent, rayonnement ...)
    * todo: données supplémentaires ? https://www.copernicus.eu/en

* Geo
    * CONTOURS...IRIS (chosen, best if possible)
        * [source](https://geoservices.ign.fr/contoursiris)
        * [documentation](https://geoservices.ign.fr/documentation/donnees/vecteur/contoursiris)
        * todo: is IRIS GE better for the project ?
        * correspondance code commune : 5 premiers chiffres + 4 spécifiques à l'IRIS
        * todo fréquence de mise à jour ?
    * ADMIN EXPRESS → COG (2nd best if possible)
        * [source](https://geoservices.ign.fr/adminexpress) → updated once per month
        * [documentation](https://geoservices.ign.fr/documentation/donnees/vecteur/adminexpress)
        * todo: est-ce utile ? certainement pour mieux géolocaliser la commune
        * todo: contient les EPCI ?
    * might be util ?
        * https://github.com/InseeFrLab/pynsee
    * EPCI pas traités, car faible volume de données avec EPCI mais sans code_iris ou code_insee
        * 1230 lignes
        * 667.569 -> puissance ignorée
        * todo: mesurer ceci en data quality
    * todo: modèle de données
        * code_iris (unique, pk)
        * code_insee (fk -> insee)
        * (iris) les quatre derniers chiffres spécifiques à l'iris, utile ?
        * nom_iris
        * nom_commune
        * type_iris (Habitat, Activité, Divers, Z (pas découpé, équivalent à code_insee))

### Todo

* énergie
    * https://data.rte-france.com/
        * problème d'inscription ?
    * https://www.data.gouv.fr/pages/donnees-energie/
        * page générale pour les données de production/consommation d'énergie (pas uniquement électrique)
    * https://www.data.gouv.fr/datasets/generation-forecast
        * prévisionnel J+3 J+2 J+1 si besoin ?

    * données production/consommation régionales
        * (temps
          réel) https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-tr/information/?disjunctive.nature&disjunctive.libelle_region
            * (explications) https://www.rte-france.com/donnees-publications/eco2mix-donnees-temps-reel
        * (consolidées &
          définitives) https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-cons-def/information/?disjunctive.nature&disjunctive.libelle_region

* géographique
    * relier commune/iris/epci/... (trouver la maille la plus fine accessible)
        * https://public.opendatasoft.com/explore/assets/georef-france-commune-arrondissement-municipal/
        * https://public.opendatasoft.com/explore/assets/georef-france-commune-millesime/
        * https://www.data.gouv.fr/datasets/referentiel-geographique-francais-communes-unites-urbaines-aires-urbaines-departements-academies-regions
        * https://www.insee.fr/fr/information/7708995
        * https://www.data.gouv.fr/datasets/admin-express-admin-express-cog-admin-express-cog-carto-admin-express-cog-carto-pe-admin-express-cog-carto-plus-pe
        * www.data.gouv.fr/datasets/iris-ge
        * https://gitlab.com/Oslandia/pyris

* météorologiques
    * (base des deux API disponible) https://donneespubliques.meteofrance.fr/
        * exploration https://github.com/loicduffar/meteo.data-Tools
        * https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-horaires
            * (identique normalement) https://meteo.data.gouv.fr/datasets/6569b4473bedf2e7abad3b72
        * https://www.data.gouv.fr/datasets/liste-des-stations-en-open-data-du-reseau-meteorologique-infoclimat-static-et-meteo-france-synop
            * pas de Météo France
        * https://www.infoclimat.fr/opendata/stations_xhr.php?format=geojson
            * liste

    * data quality des données
        * https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/621510657/Donn+es+climatologiques+de+base
            * par exemple, les stations météo de type 0 sont les plus précises, y a-t-il une différence flagrante de
              précision avec les autres et est-ce que ça impacte la qualité de l'analyse ?