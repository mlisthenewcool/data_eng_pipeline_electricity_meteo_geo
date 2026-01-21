import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from de_electricity_meteo.config.paths import METEO_FRANCE_INFO_STATIONS_BRONZE, DATA_BRONZE

    return DATA_BRONZE, METEO_FRANCE_INFO_STATIONS_BRONZE, pl


@app.cell
def _(METEO_FRANCE_INFO_STATIONS_BRONZE, pl):
    df = pl.read_json(METEO_FRANCE_INFO_STATIONS_BRONZE)

    print(df.shape)
    print(df.columns)

    df
    return (df,)


@app.cell
def _(df, pl):
    df_with_only_active_stations = df.filter(
        pl.col("dateFin").is_null() | (pl.col("dateFin") == "")
    )
    df_with_only_active_stations  # 2397 stations actives
    return (df_with_only_active_stations,)


@app.cell
def _(df_with_only_active_stations, pl):
    tmp_df = df_with_only_active_stations.with_columns(
        pl.col("typesPoste")
        .list.eval(
            (pl.element().struct.field("type") == 0)
            & (
                pl.element().struct.field("dateFin").is_null()
                | (pl.element().struct.field("dateFin") == "")
            )
        )
        .list.sum()
        .alias("n_postes_actifs_type_0")
    )

    """
    df_with_only_active_stations_and_type_0 = (
        df_with_only_active_stations
        .explode("typesPoste") # Transforme chaque élément de liste en une ligne dédiée
        .filter(
            (pl.col("typesPoste").struct.field("type") == 0) & 
            (pl.col("typesPoste").struct.field("dateFin").is_null() | 
             (pl.col("typesPoste").struct.field("dateFin") == ""))
        )
    )
    """

    print(
        "number of type 0 stations with more than 1 open 'poste' -- should be 0:",
        tmp_df.filter(pl.col("n_postes_actifs_type_0") > 1).shape,
    )

    df_with_only_active_stations_and_type_0 = tmp_df.filter(pl.col("n_postes_actifs_type_0") == 1)
    return (df_with_only_active_stations_and_type_0,)


@app.cell
def _(df_with_only_active_stations_and_type_0):
    df_with_only_active_stations_and_type_0
    return


@app.cell
def _(df_with_only_active_stations_and_type_0, pl):
    condition_exclusion = (pl.element().struct.field("dateFin").is_not_null()) & (
        pl.element().struct.field("dateFin") != ""
    )

    df_with_only_active_stations_and_type_0_keep_only_active_parameters = (
        df_with_only_active_stations_and_type_0.with_columns(
            pl.col("parametres")
            .list.eval(pl.element().filter(~condition_exclusion))
            .alias("parametres_actifs")
        )
    )

    df_with_only_active_stations_and_type_0_keep_only_active_parameters
    return (df_with_only_active_stations_and_type_0_keep_only_active_parameters,)


@app.cell
def _(df_with_only_active_stations_and_type_0_keep_only_active_parameters, pl):
    print(
        df_with_only_active_stations_and_type_0_keep_only_active_parameters.select(
            pl.col("parametres_actifs").list.len().max()
        )
    )
    print(
        df_with_only_active_stations_and_type_0_keep_only_active_parameters.select(
            pl.col("parametres").list.len().max()
        )
    )
    return


@app.cell
def _(df_with_only_active_stations_and_type_0_keep_only_active_parameters, pl):
    liste_parametres_actifs = (
        df_with_only_active_stations_and_type_0_keep_only_active_parameters.select(
            pl.col("parametres_actifs").list.eval(pl.element().struct.field("nom"))
        )
        .explode("parametres_actifs")
        .unique()
    )["parametres_actifs"].to_list()

    liste_parametres_actifs
    return (liste_parametres_actifs,)


@app.cell
def _():
    selection_meteo_renouvelables = {
        # --- SOLAIRE PHOTOVOLTAÏQUE (PV) ---
        "RAYONNEMENT GLOBAL HORAIRE": True,  # Crucial : Somme du rayonnement direct et diffus, base du calcul de production PV.
        "RAYONNEMENT GLOBAL QUOTIDIEN": True,  # Utile pour des bilans de production journaliers.
        "RAYONNEMENT DIRECT HORAIRE": True,  # Permet d'affiner le calcul pour les panneaux inclinés ou avec trackers.
        "DUREE D'INSOLATION HORAIRE": True,  # Indicateur de nébulosité réelle impactant le gisement solaire.
        "NEBULOSITE TOTALE HORAIRE": True,  # L'ennemi du PV : permet de modéliser les chutes de tension dues aux nuages.
        "TEMPERATURE SOUS ABRI HORAIRE": True,  # Important : le rendement des cellules PV baisse quand la température monte.
        "TEMPERATURE MAXIMALE SOUS ABRI HORAIRE": True,  # Pour estimer les pics de chaleur et la baisse de performance associée.
        # --- ÉOLIEN ---
        "VITESSE DU VENT HORAIRE": True,  # La base : la production éolienne varie selon le cube de la vitesse du vent.
        "MOYENNE DES VITESSES DU VENT A 10M": True,  # Standard de mesure pour extrapoler la vitesse à hauteur de turbine.
        "DIRECTION DU VENT A 10 M HORAIRE": True,  # Crucial pour l'orientation des nacelles et l'étude des effets de sillage.
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE SUR 3 SECONDES": True,  # Pour la sécurité : détecter les seuils de coupure (tempête).
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE SUR 3 SECONDES": True,  # Analyse de la turbulence et des contraintes mécaniques.
        "NOMBRE DE JOURS AVEC FXY>=10 M/S": True,  # Indicateur de potentiel de productibilité élevé.
        "NOMBRE DE JOURS AVEC FXY>=8 M/S": True,  # Seuil souvent proche du début de la puissance nominale des éoliennes.
        # --- AUTRES PARAMÈTRES (A mettre à False par défaut) ---
        "HEURE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,  # Trop agrégé pour de la simulation précise.
        "MOYENNE DECADAIRE DE LA TEMPERATURE MAXI": False,  # Échelle de temps trop longue (10 jours).
        "SOMME DES TNTXM QUOTIDIEN SUP A 6°C": False,  # Paramètre agronomique (croissance plantes).
        "NOMBRE DE JOURS AVEC FXY>=15 M/S": False,  # Utile pour l'usure, mais moins pour l'estimation de prod moyenne.
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "NOMBRE DE JOURS AVEC TX<=20°C": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 200 MM": False,  # Hydrologie/Agriculture.
        "HEURE DU TX SOUS ABRI HORAIRE": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 50 MM": False,
        "MOYENNE DECADAIRE DE LA TEMPERATURE MINI": False,
        "DIRECTION DU VENT MOYEN SUR 10 MN MAXIMAL HORAIRE": False,
        "DUREE HUMECTATION QUOTIDIENNE": False,
        "BASE DE LA 2EME COUCHE NUAGEUSE": False,
        "VITESSE VENT MAXI INSTANTANE SUR 3 SECONDES": False,
        "DUREE DES PRECIPITATIONS HORAIRE": False,
        "CUMUL DES DUREES D'INSOLATION": False,
        "TEMPERATURE MAXIMALE SOUS ABRI HORAIRE": True,  # Déjà mis à True plus haut
        "NOMBRE DE JOURS AVEC TN<=+20°C": False,
        "NOMBRE DE JOURS AVEC FXI>=28 M/S": False,  # Sécurité uniquement.
        "NOMBRE DE JOURS AVEC TN>=+25°C": False,
        "ETP CALCULEE AU POINT DE GRILLE LE PLUS PROCHE": False,  # Évapotranspiration (Agriculture).
        "VITESSE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "TEMPERATURE A -10 CM HORAIRE": False,  # Température du sol.
        "HEURE DU TN SOUS ABRI QUOTIDIENNE": False,
        "VISIBILITE HORAIRE": False,
        "NEBULOSITE DE LA 1ERE COUCHE NUAGEUSE": False,
        "TEMPERATURE DE CHAUSSEE": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 3 HEURES": False,
        "CODE TEMPS PRESENT HORAIRE": False,
        "OCCURRENCE DE FUMEE QUOTIDIENNE": False,
        "EPAISSEUR DE NEIGE TOTALE HORAIRE": False,  # La neige sur les panneaux est un facteur, mais marginal vs rayonnement.
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE": False,
        "RAPPORT INSOLATION QUOTIDIEN": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 6 HEURES": False,
        "SOMME DES ETP PENMAN": False,
        "NOMBRE DE JOURS AVEC TN<=+10°C": False,
        "TEMPERATURE A -20 CM HORAIRE": False,
        "NOMBRE DE JOURS AVEC FXI3S>=10M/S": False,
        "HEURE DU VENT MAX INSTANTANE HORAIRE": False,
        "PRESSION STATION HORAIRE": False,  # La densité de l'air influe sur l'éolien, mais peu de variation vs vitesse.
        "EPAISSEUR DE NEIGE TOTALE RELEVEE A 0600 FU": False,
        "CUMUL DECADAIRE DES TM>6 AVEC TM ECRETEE A 30 POUR TX": False,
        "MAXIMUM QUOTIDIEN DES EPAISSEURS DE NEIGE TOTALE HORAIRE": False,
        "HUMIDITE RELATIVE MINI MENSUELLE": False,
        "ETAT DE LA MER HORAIRE": False,
        "CODE TEMPS PASSE W2 HORAIRE": False,
        "NEBULOSITE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 100 MM": False,
        "HAUTEUR DE PRECIPITATIONS HORAIRE": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 200 MM": False,
        "OCCURRENCE DE BROUILLARD QUOTIDIENNE": False,
        "AMPLITUDE ENTRE TN ET TX QUOTIDIEN": False,
        "HEURE DU TX SOUS ABRI QUOTIDIENNE": False,
        "DUREE HUMIDITE >= 80% QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC TX>=30°C": False,
        "CUMUL DECADAIRE DES TM>8 AVEC TM ECRETEE A 30 POUR TX": False,
        "NOMBRE DE JOURS AVEC TX>=25°C": False,
        "DUREE D'INSOLATION HORAIRE": True,  # Déjà mis à True plus haut
        "MOYENNE DES TM": False,
        "ETP PENMAN DECADAIRE": False,
        "NEBUL. DE LA COUCHE NUAG. PRINCIPALE LA PLUS BASSE HORAIRE": False,
        "NOMBRE DE JOURS AVEC RR>=30 MM": False,
        "CUMUL DECADAIRE DES TM>0 AVEC TM NON ECRETEE": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 150 MM": False,
        "CODE SYNOP NUAGES ELEVE HORAIRE": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE SUR 3 SECONDES": False,
        "DUREE HUMIDITE<=40% HORAIRE": False,
        "HEURE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": False,
        "HUMIDITE RELATIVE MOYENNE": False,
        "DUREE D'INSOLATION QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MAXIMALE QUOTIDIENNE": False,
        "MOYENNE DECADAIRE DE LA FORCE DU VENT": False,
        "GEOPOTENTIEL HORAIRE": False,
        "NOMBRE DE JOURS AVEC TX>=35°C": False,
        "NOMBRE DE JOURS AVEC FXI3S>=28M/S": False,
        "MOYENNE DES VITESSES DU VENT A 10M QUOTIDIENNE": False,
        "DUREE HUMIDITE>=80% HORAIRE": False,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE SUR 3 SECONDES": True,  # Déjà à True
        "NOMBRE DE JOURS AVEC TX<=0°C": False,
        "EVAPO-TRANSPIRATION MONTEITH QUOTIDIENNE": False,
        "MOYENNE DES VITESSES DU VENT A 10M": True,  # Déjà à True
        "NOMBRE DE JOURS AVEC RR>=5 MM": False,
        "MOYENNE DECADAIRE DE LA TENSION DE VAPEUR": False,
        "NEBULOSITE DE LA 4EME COUCHE NUAGEUSE": False,
        "RAYONNEMENT INFRA-ROUGE HORAIRE": False,
        "TENSION DE VAPEUR MOYENNE": False,
        "MINIMUM DES TX DU MOIS": False,
        "RAYONNEMENT ULTRA VIOLET HORAIRE": False,
        "RAYONNEMENT GLOBAL QUOTIDIEN": True,  # Déjà à True
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 100 MM": False,
        "BASE DE LA 1ERE COUCHE NUAGEUSE": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 8°C": False,
        "DUREE DE GEL QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC RR>=100 MM": False,
        "MAX DES FXY QUOTIDIEN": False,
        "NOMBRE DE JOURS AVEC GRELE": False,
        "DIRECTION DU VENT MAXI INSTANTANE SUR 3 SECONDES": False,
        "NOMBRE DE JOURS AVEC RR>=50 MM": False,
        "CUMUL DES DJU SEUIL 18 METHODE METEO": False,  # Utile pour la CONSO (chauffage), pas pour la PROD.
        "RAYONNEMENT DIRECT HORAIRE EN TEMPS SOLAIRE VRAI": True,  # Très bon pour le solaire si dispo.
        "NOMBRE DE JOURS AVEC TN<=-5°C": False,
        "BASE DE LA 4EME COUCHE NUAGEUSE": False,
        "CUMUL DES HAUTEURS DE PRECIPITATIONS": False,
        "DUREE AVEC VISIBILITE<200 M": False,
        "OCCURRENCE D'ORAGE QUOTIDIENNE": False,
        "ETAT DU SOL AVEC NEIGE HORAIRE": False,
        "NEBULOSITE TOTALE HORAIRE": True,  # Déjà à True
        "CODE TEMPS PASSE W1 HORAIRE": False,
        "DUREE TOTALE D'INSOLATION DECADAIRE": False,
        "RAYONNEMENT INFRA-ROUGE HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "HAUTEUR TOTALE DECADAIRE DES PRECIPITATIONS": False,
        "EPAISSEUR MAXIMALE DE NEIGE": False,
        "HEURE DU MAXI D'HUMIDITE QUOTIDIENNE": False,
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN": False,
        "HEURE DU MINI D'HUMIDITE QUOTIDIENNE": False,
        "NEBULOSITE DE LA 3EME COUCHE NUAGEUSE": False,
        "VISIBILITE VERS LA MER": False,
        "CUMUL DU RAYONNEMENT GLOBAL QUOTIDIEN": False,
        "OCCURRENCE DE GRELE QUOTIDIENNE": False,
        "BASE DE LA 3EME COUCHE NUAGEUSE": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 0°C": False,
        "OCCURRENCE DE BRUME QUOTIDIENNE": False,
        "VITESSE DU VENT HORAIRE": True,  # Déjà à True
        "MOYENNE DES (TN+TX)/2": False,
        "HUMIDITE RELATIVE MAXI HORAIRE": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 200 MM": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE": False,
        "TEMPERATURE MINIMALE SOUS ABRI HORAIRE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 50 MM": False,
        "SOMME DES RAYONNEMENTS IR HORAIRE": False,
        "NOMBRE DE JOURS AVEC SIGMA<=20%": False,
        "TEMPERATURE MINIMALE A +10CM QUOTIDIENNE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 200 MM": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 1 HEURE": False,
        "RAYONNEMENT ULTRA VIOLET QUOTIDIEN": False,
        "HUMIDITE RELATIVE MAXI MENSUELLE": False,
        "NOMBRE DE JOURS AVEC TN<=+15°C": False,
        "MAX DES INDICES UV HORAIRE": False,
        "RAYONNEMENT ULTRA VIOLET HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "BASE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "NOMBRE DE JOURS AVEC TN<=-10°C": False,
        "NOMBRE DE JOURS AVEC TM>=+24°C": False,
        "DUREE DE GEL HORAIRE": False,
        "VITESSE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": False,
        "RAYONNEMENT GLOBAL HORAIRE EN TEMPS SOLAIRE VRAI": True,  # Très bon pour le solaire.
        "NOMBRE DE JOURS AVEC FXY>=8 M/S": True,  # Déjà à True
        "TEMPERATURE MOYENNE SOUS ABRI QUOTIDIENNE": False,
        "DUREE DES PRECIPITATIONS QUOTIDIENNES": False,
        "INDICE UV HORAIRE (COMPRIS ENTRE 0 ET 12)": False,
        "NOMBRE DE JOURS AVEC SIGMA=0%": False,
        "OCCURRENCE DE ROSEE QUOTIDIENNE": False,
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN": False,
        "CUMUL DE PRECIPITATIONS EN 6 MN": False,
        "MOYENNE DES TN DU MOIS": False,
        "PRECIPITATION MAXIMALE EN 24H": False,
        "RAYONNEMENT GLOBAL HORAIRE": True,  # Déjà à True
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 150 MM": False,
        "NBRE DE JOURS PRESENTS AVEC ORAGE": False,
        "QUANTITE DE PRECIPITATIONS LORS DE L'EPISODE PLUVIEUX": False,
        "NOMBRE DE JOURS AVEC TX>=32°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 10°C": False,
        "HEURE DE L'HUMIDITE RELATIVE MAXIMALE HORAIRE": False,
        "DIRECTION DU VENT A 10 M HORAIRE": True,  # Déjà à True
        "RAYONNEMENT DIRECT QUOTIDIEN": True,
        "VITESSE VENT MAXI INSTANTANE": False,
        "TEMPERATURE MINIMALE SOUS ABRI QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC FXI>=16 M/S": False,
        "TENSION DE VAPEUR HORAIRE": False,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE": False,
        "TYPE DE LA 3EME COUCHE NUAGEUSE": False,
        "ETAT DU SOL SANS NEIGE HORAIRE": False,
        "TX MAXI DU MOIS": False,
        "NOMBRE DE JOURS AVEC GELEE": False,
        "HAUTEUR DE PRECIPITATIONS QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC TX<=27°C": False,
        "TEMPERATURE SOUS ABRI HORAIRE": True,  # Déjà à True
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 100 MM": False,
        "CUMUL DU RAYONNEMENT DIRECT QUOTIDIEN": False,
        "PRESSION MER HORAIRE": False,
        "DUREE D'INSOLATION HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "TEMPERATURE MAXIMALE SOUS ABRI QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MINI HORAIRE": False,
        "TEMPERATURE MINIMALE A +10CM HORAIRE": False,
        "MINIMUM DE LA PRESSION MER": False,
        "TEMPERATURE DU POINT DE ROSEE HORAIRE": False,
        "TYPE DE LA 2EME COUCHE NUAGEUSE": False,
        "TN MINI DU MOIS": False,
        "PRESSION MER MINIMUM QUOTIDIENNE": False,
        "NOMBRE DE JOURS AVEC SOL COUVERT DE NEIGE": False,
        "NOMBRE DE JOURS AVEC RR>=1 MM": False,
        "MOYENNE DES TX DU MOIS": False,
        "CUMUL DECADAIRE DES TM>10, TM ECRETEE A 30 POUR TX, 10 POUR TN": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 100 MM": False,
        "MOYENNE DES PRESSIONS MER": False,
        "HUMIDITE RELATIVE HORAIRE": False,
        "NOMBRE DE JOURS AVEC SIGMA>=80%": False,
        "MINIMUM ABSOLU DES PMERM": False,
        "MAXIMUM DES TN DU MOIS": False,
        "HAUTEUR DE NEIGE FRAICHE TOMBEE EN 24H": False,
        "TEMPERATURE A -50 CM HORAIRE": False,
        "NEBULOSITE DE LA 2EME COUCHE NUAGEUSE": False,
        "NOMBRE DE JOURS AVEC FXI>=10 M/S": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 50 MM": False,
        "RAYONNEMENT DIRECT HORAIRE": True,  # Déjà à True
        "DUREE HUMIDITE <= 40% QUOTIDIENNE": False,
        "CODE SYNOP NUAGES BAS HORAIRE": False,
        "HEURE DE L'HUMIDITE RELATIVE MINIMALE HORAIRE": False,
        "NOMBRE DE JOURS AVEC RR>=10 MM": False,
        "HUMIDITE RELATIVE MINIMALE QUOTIDIENNE": False,
        "OCCURRENCE DE SOL COUVERT DE NEIGE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 150 MM": False,
        "OCCURRENCE DE NEIGE QUOTIDIENNE": False,
        "DIRECTION DE LA HOULE HORAIRE": False,
        "TEMPERATURE MINI A +50CM QUOTIDIENNE": False,
        "NBRE DE JOURS PRESENT AVEC BROUILLARD": False,
        "NOMBRE DE JOURS AVEC FXI3S>=16M/S": False,
        "OCCURRENCE DE GELEE BLANCHE QUOTIDIENNE": False,
        "OCCURRENCE ECLAIR QUOTIDIENNE": False,
        "HEURE DU TN SOUS ABRI HORAIRE": False,
        "TEMPERATURE MINI A +50CM HORAIRE": False,
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE SUR 3 SECONDES": True,  # Déjà à True
        "PRESSION MER MOYENNE QUOTIDIENNE": False,
        "DIRECTION VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "CODE SYNOP NUAGES MOYEN HORAIRE": False,
        "DIRECTION DU VENT MAXI INSTANTANE": False,
        "CUMUL DES DJU SEUIL 18 METHODE CHAUFFAGISTE": False,
        "TEMPERATURE A -100 CM HORAIRE": False,
        "NOMBRE DE JOURS AVEC NEIGE": False,
        "OCCURRENCE DE GRESIL QUOTIDIENNE": False,
        "OCCURRENCE DE VERGLAS": False,
        "TENSION DE VAPEUR MOYENNE QUOTIDIENNE": False,
        "CUMUL DE RAYONNEMENT GLOBAL DECADAIRE": False,
        "TYPE DE LA 1ERE COUCHE NUAGEUSE": False,
        "NOMBRE DE JOURS AVEC TN>=+20°C": False,
        "NOMBRE DE JOURS AVEC FXY>=10 M/S": True,  # Déjà à True
        "HEURE DU VENT MAX INSTANTANE HORAIRE SUR 3 SECONDES": False,
        "TYPE DE LA 4EME COUCHE NUAGEUSE": False,
        "DUREE HUMECTATION": False,
    }
    return (selection_meteo_renouvelables,)


@app.cell
def _(liste_parametres_actifs, selection_meteo_renouvelables):
    def check_if_parameters_missing(lst1: list[str], lst2: list[str]) -> bool:
        set1, set2 = set(lst1), set(lst2)
        missing_in_1 = set1 - set2
        missing_in_2 = set2 - set1

        if not missing_in_1 and not missing_in_2:
            print("OK!")
            return True

        if missing_in_1:
            print(f"n missing in original {len(missing_in_1)}")
            print(missing_in_1)

        if missing_in_2:
            print(f"n missing in AI generated {len(missing_in_2)}")
            print(missing_in_2)

        return False

    check_if_parameters_missing(liste_parametres_actifs, list(selection_meteo_renouvelables.keys()))

    # ---
    # Génère un dict par défaut avec tout à False pour les nouveaux paramètres
    # nouveaux_params = {nom: False for nom in manquants_dans_dict}
    # print(nouveaux_params)
    return (check_if_parameters_missing,)


@app.cell
def _(df_with_only_active_stations, pl):
    _df = df_with_only_active_stations.with_columns(
        pl.col("typesPoste")
        .list.eval(
            pl.element().struct.field("dateFin").is_null()
            | (pl.element().struct.field("dateFin") == "")
        )
        .list.sum()
        .alias("n_postes_actifs")
    )

    # df_with_only_active_stations_and_only_active_parameters

    # data quality
    print(
        "number of stations with more than 1 open 'poste' -- should be 0:",
        _df.filter((pl.col("n_postes_actifs") > 1) | (pl.col("n_postes_actifs") < 1)).shape,
    )
    __df = _df.filter(pl.col("n_postes_actifs") == 1)
    return (__df,)


@app.cell
def _(__df, pl):
    # ---
    plus_actif_condition_exclusion = (pl.element().struct.field("dateFin").is_not_null()) & (
        pl.element().struct.field("dateFin") != ""
    )

    ___df = __df.with_columns(
        pl.col("parametres")
        .list.eval(pl.element().filter(~plus_actif_condition_exclusion))
        .alias("parametres_actifs"),
        pl.col("typesPoste")
        .list.eval(pl.element().filter(~plus_actif_condition_exclusion))
        .alias("type_poste_actif"),
    )

    ___df
    return (___df,)


@app.cell
def _(___df, pl):
    liste_parametres_actifs_toutes_stations_ouvertes = (
        (
            ___df.select(pl.col("parametres_actifs").list.eval(pl.element().struct.field("nom")))
            .explode("parametres_actifs")
            .drop_nulls()  # todo: check where there is empty "nom" for parameters
            .unique()
        )
        .to_series()
        .sort()
        .to_list()
    )

    liste_parametres_actifs_toutes_stations_ouvertes
    # selection_meteo_renouvelables_v2
    return (liste_parametres_actifs_toutes_stations_ouvertes,)


@app.cell
def _():
    selection_meteo_renouvelables_v2 = {
        # --- SOLAIRE PHOTOVOLTAÏQUE (Gisement et Rendement) ---
        "RAYONNEMENT GLOBAL HORAIRE": True,  # La source principale d'énergie pour le PV.
        "RAYONNEMENT GLOBAL HORAIRE EN TEMPS SOLAIRE VRAI": True,  # Idéal pour corréler avec la position exacte du soleil.
        "RAYONNEMENT DIRECT HORAIRE": True,  # Crucial pour le solaire thermodynamique ou les panneaux avec trackers.
        "RAYONNEMENT DIRECT HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "DUREE D'INSOLATION HORAIRE": True,  # Permet d'estimer les passages nuageux et la stabilité de la production.
        "DUREE D'INSOLATION HORAIRE EN TEMPS SOLAIRE VRAI": True,
        "NEBULOSITE TOTALE HORAIRE": True,  # Impact direct sur la diffusion du rayonnement.
        "TEMPERATURE SOUS ABRI HORAIRE": True,  # Indispensable : la chaleur réduit l'efficacité des cellules PV.
        "TEMPERATURE MAXIMALE SOUS ABRI HORAIRE": True,  # Pour anticiper les baisses de rendement lors des pics thermiques.
        "TEMPERATURE DU POINT DE ROSEE HORAIRE": True,  # Utile pour modéliser la formation de condensation/rosée sur les panneaux.
        # --- ÉOLIEN (Force, Direction et Densité) ---
        "VITESSE DU VENT HORAIRE": True,  # Paramètre n°1 pour la courbe de puissance.
        "DIRECTION DU VENT A 10 M HORAIRE": True,  # Pour l'orientation des turbines (yaw control).
        "MOYENNE DES VITESSES DU VENT A 10M": True,  # Base standard pour le calcul du gisement local.
        "VITESSE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": True,  # Pour évaluer la stabilité du flux d'air.
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE SUR 3 SECONDES": True,  # Sécurité : détection des rafales entraînant l'arrêt des pales.
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE SUR 3 SECONDES": True,  # Analyse des contraintes mécaniques sur la structure.
        "DIRECTION DU VENT MAXI INSTANTANE SUR 3 SECONDES": True,  # Crucial pour évaluer la turbulence et la stabilité du flux : une direction de rafale instable réduit l'efficacité de l'alignement de la nacelle (yaw) et donc la production réelle.
        "VITESSE DU VENT A 2 METRES HORAIRE": True,  # Complément pour modéliser le profil vertical du vent (cisaillement).
        "DIRECTION DU VENT A 2 METRES HORAIRE": True,
        "PRESSION STATION HORAIRE": True,  # La densité de l'air dépend de la pression ; elle impacte directement la force exercée sur les pales.
        # --- INDICATEURS DE POTENTIEL (Statistiques) ---
        "NOMBRE DE JOURS AVEC FXY>=8 M/S": True,  # Seuil de productivité intéressante pour le petit et grand éolien.
        "NOMBRE DE JOURS AVEC FXY>=10 M/S": True,
        "RAYONNEMENT GLOBAL QUOTIDIEN": True,  # Utile pour valider les modèles de production journaliers.
        "RAYONNEMENT DIRECT QUOTIDIEN": True,
        # --- PARAMÈTRES REJETÉS (Justifications en commentaires) ---
        "AMPLITUDE ENTRE TN ET TX QUOTIDIEN": False,  # Trop agrégé.
        "BASE DE LA 1ERE COUCHE NUAGEUSE": False,  # Trop spécifique (aviation).
        "BASE DE LA 2EME COUCHE NUAGEUSE": False,
        "BASE DE LA 3EME COUCHE NUAGEUSE": False,
        "BASE DE LA 4EME COUCHE NUAGEUSE": False,
        "BASE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "CODE SYNOP NUAGES BAS HORAIRE": False,  # Donnée codée, moins exploitable que la nébulosité totale.
        "CODE SYNOP NUAGES ELEVE HORAIRE": False,
        "CODE SYNOP NUAGES MOYEN HORAIRE": False,
        "CODE TEMPS PASSE W1 HORAIRE": False,
        "CODE TEMPS PASSE W2 HORAIRE": False,
        "CODE TEMPS PRESENT HORAIRE": False,
        "CUMUL DE PRECIPITATIONS EN 6 MN": False,  # Trop fin, peu d'impact sur EnR.
        "CUMUL DE RAYONNEMENT GLOBAL DECADAIRE": False,  # Échelle de temps trop longue (10 jours).
        "CUMUL DECADAIRE DES TM>0 AVEC TM NON ECRETEE": False,  # Agronomie.
        "CUMUL DECADAIRE DES TM>10, TM ECRETEE A 30 POUR TX, 10 POUR TN": False,
        "CUMUL DECADAIRE DES TM>6 AVEC TM ECRETEE A 30 POUR TX": False,
        "CUMUL DECADAIRE DES TM>8 AVEC TM ECRETEE A 30 POUR TX": False,
        "CUMUL DES DJU SEUIL 18 METHODE CHAUFFAGISTE": False,  # Consommation, pas production.
        "CUMUL DES DJU SEUIL 18 METHODE METEO": False,
        "CUMUL DES DUREES D'INSOLATION": False,
        "CUMUL DES HAUTEURS DE PRECIPITATIONS": False,
        "CUMUL DU RAYONNEMENT DIRECT QUOTIDIEN": False,
        "CUMUL DU RAYONNEMENT GLOBAL QUOTIDIEN": False,
        "DIRECTION DE LA HOULE HORAIRE": False,  # Éolien offshore uniquement (très spécifique).
        "DIRECTION DU VENT INSTANTANE MAXI HORAIRE A 2 M": False,
        "DIRECTION DU VENT INSTANTANE MAXI QUOTIDIEN A 2 M": False,
        "DIRECTION DU VENT MAXI INSTANTANE": False,
        "DIRECTION DU VENT MAXI INSTANTANE HORAIRE": False,
        "DIRECTION DU VENT MOYEN SUR 10 MN MAXIMAL HORAIRE": False,
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN": False,
        "DIRECTION VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "DIRECTION VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "DUREE AVEC VISIBILITE<200 M": False,
        "DUREE D'INSOLATION QUOTIDIENNE": False,
        "DUREE DE GEL HORAIRE": False,
        "DUREE DE GEL QUOTIDIENNE": False,
        "DUREE DES PRECIPITATIONS HORAIRE": False,
        "DUREE DES PRECIPITATIONS QUOTIDIENNES": False,
        "DUREE HUMECTATION": False,
        "DUREE HUMECTATION QUOTIDIENNE": False,
        "DUREE HUMIDITE <= 40% QUOTIDIENNE": False,
        "DUREE HUMIDITE >= 80% QUOTIDIENNE": False,
        "DUREE HUMIDITE<=40% HORAIRE": False,
        "DUREE HUMIDITE>=80% HORAIRE": False,
        "DUREE TOTALE D'INSOLATION DECADAIRE": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 100 MM": False,  # Hydrologie.
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 150 MM": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 200 MM": False,
        "ECOULEMENT D'EAU DECADAIRE POUR UNE RESERVE UTILE DE 50 MM": False,
        "ENFONCEMENT DU TUBE DE NEIGE": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 1 HEURE": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 3 HEURES": False,
        "EPAISSEUR DE NEIGE FRAICHE SUR 6 HEURES": False,
        "EPAISSEUR DE NEIGE TOTALE HORAIRE": False,  # Impact possible (albédo ou obstruction), mais secondaire.
        "EPAISSEUR DE NEIGE TOTALE RELEVEE A 0600 FU": False,
        "EPAISSEUR MAXIMALE DE NEIGE": False,
        "ETAT DE LA COUCHE SUPERFICIELLE DE NEIGE": False,
        "ETAT DE LA MER HORAIRE": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 100 MM": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 200 MM": False,
        "ETAT DECADAIRE DES RESERVES POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 100 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 200 MM": False,
        "ETAT DECADAIRE DU RESERVOIR PROFOND POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 100 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 150 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 200 MM": False,
        "ETAT DECADAIRE DU RESERVOIR SUPERIEUR POUR UNE RESERVE UTILE DE 50 MM": False,
        "ETAT DU SOL AVEC NEIGE HORAIRE": False,
        "ETAT DU SOL SANS NEIGE HORAIRE": False,
        "ETP CALCULEE AU POINT DE GRILLE LE PLUS PROCHE": False,
        "ETP PENMAN DECADAIRE": False,
        "EVAPO-TRANSPIRATION MONTEITH QUOTIDIENNE": False,
        "GEOPOTENTIEL HORAIRE": False,
        "HAUTEUR DE NEIGE FRAICHE TOMBEE EN 24H": False,
        "HAUTEUR DE PRECIPITATIONS HORAIRE": False,
        "HAUTEUR DE PRECIPITATIONS QUOTIDIENNE": False,
        "HAUTEUR ESTIMEE DES PRECIPS MENSUELLES": False,
        "HAUTEUR TOTALE DECADAIRE DES PRECIPITATIONS": False,
        "HEURE DE L'HUMIDITE RELATIVE MAXIMALE HORAIRE": False,
        "HEURE DE L'HUMIDITE RELATIVE MINIMALE HORAIRE": False,
        "HEURE DU MAXI D'HUMIDITE QUOTIDIENNE": False,
        "HEURE DU MINI D'HUMIDITE QUOTIDIENNE": False,
        "HEURE DU TN SOUS ABRI HORAIRE": False,
        "HEURE DU TN SOUS ABRI QUOTIDIENNE": False,
        "HEURE DU TX SOUS ABRI HORAIRE": False,
        "HEURE DU TX SOUS ABRI QUOTIDIENNE": False,
        "HEURE DU VENT MAX INSTANTANE A 2 M HORAIRE": False,
        "HEURE DU VENT MAX INSTANTANE A 2 M QUOTIDIENNE": False,
        "HEURE DU VENT MAX INSTANTANE HORAIRE": False,
        "HEURE DU VENT MAX INSTANTANE HORAIRE SUR 3 SECONDES": False,
        "HEURE DU VENT MOYEN SUR 10 MN MAXI HORAIRE": False,
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN": False,
        "HEURE VENT MAXI INSTANTANE QUOTIDIEN SUR 3 SECONDES": False,
        "HEURE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
        "HUMIDITE RELATIVE HORAIRE": False,
        "HUMIDITE RELATIVE MAXI HORAIRE": False,
        "HUMIDITE RELATIVE MAXI MENSUELLE": False,
        "HUMIDITE RELATIVE MAXIMALE QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MINI HORAIRE": False,
        "HUMIDITE RELATIVE MINI MENSUELLE": False,
        "HUMIDITE RELATIVE MINIMALE QUOTIDIENNE": False,
        "HUMIDITE RELATIVE MOYENNE": False,
        "INDICE UV HORAIRE (COMPRIS ENTRE 0 ET 12)": False,
        "MAX DES FXY QUOTIDIEN": False,
        "MAX DES INDICES UV HORAIRE": False,
        "MAXIMUM DES TN DU MOIS": False,
        "MAXIMUM QUOTIDIEN DES EPAISSEURS DE NEIGE TOTALE HORAIRE": False,
        "MINIMUM ABSOLU DES PMERM": False,
        "MINIMUM DE LA PRESSION MER": False,
        "MINIMUM DES TX DU MOIS": False,
        "MOYENNE DECADAIRE DE LA FORCE DU VENT": False,
        "MOYENNE DECADAIRE DE LA TEMPERATURE MAXI": False,
        "MOYENNE DECADAIRE DE LA TEMPERATURE MINI": False,
        "MOYENNE DECADAIRE DE LA TENSION DE VAPEUR": False,
        "MOYENNE DES (TN+TX)/2": False,
        "MOYENNE DES PRESSIONS MER": False,
        "MOYENNE DES TM": False,
        "MOYENNE DES TN DU MOIS": False,
        "MOYENNE DES TX DU MOIS": False,
        "MOYENNE DES VITESSES DU VENT A 10M QUOTIDIENNE": False,
        "MOYENNE DES VITESSES DU VENT A 2 METRES QUOTIDIENNE": False,
        "MOYENNE MENSUELLE ESTIMEE DES TN DU MOIS": False,
        "MOYENNE MENSUELLE ESTIMEE DES TX DU MOIS": False,
        "NBRE DE JOURS PRESENT AVEC BROUILLARD": False,
        "NBRE DE JOURS PRESENTS AVEC ORAGE": False,
        "NEBUL. DE LA COUCHE NUAG. PRINCIPALE LA PLUS BASSE HORAIRE": False,
        "NEBULOSITE DE LA 1ERE COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA 2EME COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA 3EME COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA 4EME COUCHE NUAGEUSE": False,
        "NEBULOSITE DE LA COUCHE NUAGEUSE LA PLUS BASSE": False,
        "NOMBRE DE JOURS AVEC FXI3S>=10M/S": False,
        "NOMBRE DE JOURS AVEC FXI3S>=16M/S": False,
        "NOMBRE DE JOURS AVEC FXI3S>=28M/S": False,
        "NOMBRE DE JOURS AVEC FXI>=10 M/S": False,
        "NOMBRE DE JOURS AVEC FXI>=16 M/S": False,
        "NOMBRE DE JOURS AVEC FXI>=28 M/S": False,
        "NOMBRE DE JOURS AVEC FXY>=10 M/S": False,
        "NOMBRE DE JOURS AVEC FXY>=15 M/S": False,
        "NOMBRE DE JOURS AVEC GELEE": False,
        "NOMBRE DE JOURS AVEC GRELE": False,
        "NOMBRE DE JOURS AVEC NEIGE": False,
        "NOMBRE DE JOURS AVEC RR>=1 MM": False,
        "NOMBRE DE JOURS AVEC RR>=10 MM": False,
        "NOMBRE DE JOURS AVEC RR>=100 MM": False,
        "NOMBRE DE JOURS AVEC RR>=30 MM": False,
        "NOMBRE DE JOURS AVEC RR>=5 MM": False,
        "NOMBRE DE JOURS AVEC RR>=50 MM": False,
        "NOMBRE DE JOURS AVEC SIGMA<=20%": False,
        "NOMBRE DE JOURS AVEC SIGMA=0%": False,
        "NOMBRE DE JOURS AVEC SIGMA>=80%": False,
        "NOMBRE DE JOURS AVEC SOL COUVERT DE NEIGE": False,
        "NOMBRE DE JOURS AVEC TM>=+24°C": False,
        "NOMBRE DE JOURS AVEC TN<=+10°C": False,
        "NOMBRE DE JOURS AVEC TN<=+15°C": False,
        "NOMBRE DE JOURS AVEC TN<=+20°C": False,
        "NOMBRE DE JOURS AVEC TN<=-10°C": False,
        "NOMBRE DE JOURS AVEC TN<=-5°C": False,
        "NOMBRE DE JOURS AVEC TN>=+20°C": False,
        "NOMBRE DE JOURS AVEC TN>=+25°C": False,
        "NOMBRE DE JOURS AVEC TX<=0°C": False,
        "NOMBRE DE JOURS AVEC TX<=20°C": False,
        "NOMBRE DE JOURS AVEC TX<=27°C": False,
        "NOMBRE DE JOURS AVEC TX>=25°C": False,
        "NOMBRE DE JOURS AVEC TX>=30°C": False,
        "NOMBRE DE JOURS AVEC TX>=32°C": False,
        "NOMBRE DE JOURS AVEC TX>=35°C": False,
        "OCCURRENCE D'ORAGE QUOTIDIENNE": False,
        "OCCURRENCE DE BROUILLARD QUOTIDIENNE": False,
        "OCCURRENCE DE BRUME QUOTIDIENNE": False,
        "OCCURRENCE DE FUMEE QUOTIDIENNE": False,
        "OCCURRENCE DE GELEE BLANCHE QUOTIDIENNE": False,
        "OCCURRENCE DE GRELE QUOTIDIENNE": False,
        "OCCURRENCE DE GRESIL QUOTIDIENNE": False,
        "OCCURRENCE DE NEIGE QUOTIDIENNE": False,
        "OCCURRENCE DE ROSEE QUOTIDIENNE": False,
        "OCCURRENCE DE SOL COUVERT DE NEIGE": False,
        "OCCURRENCE DE VERGLAS": False,
        "OCCURRENCE ECLAIR QUOTIDIENNE": False,
        "PRECIPITATION MAXIMALE EN 24H": False,
        "PRESSION MER HORAIRE": False,  # On préfère PRESSION STATION pour la densité de l'air locale.
        "PRESSION MER MINIMUM QUOTIDIENNE": False,
        "PRESSION MER MOYENNE QUOTIDIENNE": False,
        "QUANTITE DE PRECIPITATIONS LORS DE L'EPISODE PLUVIEUX": False,
        "QUANTITE PRECIP BRUTE": False,
        "RAPPORT INSOLATION QUOTIDIEN": False,
        "RAYONNEMENT INFRA-ROUGE HORAIRE": False,  # Rayonnement thermique (refroidissement), peu utile pour prod PV.
        "RAYONNEMENT INFRA-ROUGE HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "RAYONNEMENT ULTRA VIOLET HORAIRE": False,  # Le PV exploite peu les UV.
        "RAYONNEMENT ULTRA VIOLET HORAIRE EN TEMPS SOLAIRE VRAI": False,
        "RAYONNEMENT ULTRA VIOLET QUOTIDIEN": False,
        "SOMME DES ETP PENMAN": False,
        "SOMME DES RAYONNEMENTS IR HORAIRE": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 0°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 10°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 6°C": False,
        "SOMME DES TNTXM QUOTIDIEN SUP A 8°C": False,
        "TEMPERATURE A -10 CM HORAIRE": False,
        "TEMPERATURE A -100 CM HORAIRE": False,
        "TEMPERATURE A -20 CM HORAIRE": False,
        "TEMPERATURE A -50 CM HORAIRE": False,
        "TEMPERATURE DE CHAUSSEE": False,
        "TEMPERATURE DE SURFACE DE LA NEIGE": False,
        "TEMPERATURE MAXIMALE SOUS ABRI QUOTIDIENNE": False,
        "TEMPERATURE MINI A +50CM HORAIRE": False,
        "TEMPERATURE MINI A +50CM QUOTIDIENNE": False,
        "TEMPERATURE MINIMALE A +10CM HORAIRE": False,
        "TEMPERATURE MINIMALE A +10CM QUOTIDIENNE": False,
        "TEMPERATURE MINIMALE SOUS ABRI HORAIRE": False,
        "TEMPERATURE MINIMALE SOUS ABRI QUOTIDIENNE": False,
        "TEMPERATURE MOYENNE SOUS ABRI QUOTIDIENNE": False,
        "TENSION DE VAPEUR HORAIRE": False,
        "TENSION DE VAPEUR MOYENNE": False,
        "TENSION DE VAPEUR MOYENNE QUOTIDIENNE": False,
        "TN MINI DU MOIS": False,
        "TX MAXI DU MOIS": False,
        "TYPE DE LA 1ERE COUCHE NUAGEUSE": False,
        "TYPE DE LA 2EME COUCHE NUAGEUSE": False,
        "TYPE DE LA 3EME COUCHE NUAGEUSE": False,
        "TYPE DE LA 4EME COUCHE NUAGEUSE": False,
        "VISIBILITE HORAIRE": False,
        "VISIBILITE VERS LA MER": False,
        "VITESSE DU VENT A 2 METRES HORAIRE": False,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE": False,
        "VITESSE DU VENT INSTANTANE MAXI HORAIRE A 2M": False,
        "VITESSE DU VENT INSTANTANE MAXI QUOTIDIEN A 2 M": False,
        "VITESSE VENT MAXI INSTANTANE": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE": False,
        "VITESSE VENT MAXI INSTANTANE QUOTIDIENNE SUR 3 SECONDES": False,
        "VITESSE VENT MAXI INSTANTANE SUR 3 SECONDES": False,
        "VITESSE VENT QUOTIDIEN MAXI MOYENNE SUR 10 MIN": False,
    }

    selection_meteo_renouvelables_v2_uppercase = {
        k.upper(): v for k, v in selection_meteo_renouvelables_v2.items()
    }
    return (
        selection_meteo_renouvelables_v2,
        selection_meteo_renouvelables_v2_uppercase,
    )


@app.cell
def _(
    check_if_parameters_missing,
    liste_parametres_actifs_toutes_stations_ouvertes,
    selection_meteo_renouvelables_v2,
):
    check_if_parameters_missing(
        liste_parametres_actifs_toutes_stations_ouvertes,
        list(selection_meteo_renouvelables_v2.keys()),
    )
    return


@app.cell
def _(___df, pl, selection_meteo_renouvelables_v2_uppercase):
    parameters_to_keep = [
        param
        for param, should_select in selection_meteo_renouvelables_v2_uppercase.items()
        if should_select is True
    ]

    ____df = ___df.with_columns(
        pl.col("parametres_actifs")
        .list.eval(pl.element().filter(pl.element().struct.field("nom").is_in(parameters_to_keep)))
        .alias("parametres_actifs_pertinents")
    )

    ____df
    return (____df,)


@app.cell
def _(____df, pl):
    # is there any stations without any parameters left ?
    ____df.filter(pl.col("parametres_actifs_pertinents").list.len() < 0)
    return


@app.cell
def _(____df, pl):
    # checks
    # no id duplicated (should be earlier)
    print("duplicated on id: ", ____df.filter(pl.col("id").is_duplicated()).shape)

    # should have only 1 active
    print(
        "count(type_post_actif) != 1",
        ____df.filter(pl.col("type_poste_actif").list.len() != 1).shape,
    )

    # null count
    print("null_count", ____df.null_count().to_dicts()[0])
    return


@app.cell
def _(____df, pl):
    # how many stations per type

    stats_par_type = (
        ____df.explode("type_poste_actif")  # On crée une ligne par élément de la liste
        .drop_nulls("type_poste_actif")  # On retire les éventuelles listes vides
        .group_by(pl.col("type_poste_actif").struct.field("type"))
        .agg(pl.len().alias("nb_stations"))
        .sort("type")
    )

    print(stats_par_type)

    # should be ok to keep only [0:2]
    # as we can read there : https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/621510657/Donn+es+climatologiques+de+base
    # type = [0:2]
    return


@app.cell
def _(____df, pl):
    # next steps

    # reorder and rename columns
    column_mapping = {
        "id": "id",  # vérifier si les 5 premiers caractères sont bien identiques au code INSEE (grâce au nom par ex ?)
        "nom": "nom",  # à mettre en relation avec le référentiel INSEE ?
        "lieuDit": "lieu_dit",  # est-ce important ?
        "bassin": "bassin",  # qu'est-ce que c'est ?
        "dateDebut": "date_debut",
        "dateFin": "date_fin",
        "positions": "positions",  # todo:
        "typesPoste": "types_poste",  # todo: applatir sur le dernier actif
        "n_postes_actifs": "n_postes",  # todo: à retirer
        "type_poste_actif": "type_poste_actif",  # todo: ?
        "producteurs": "producteurs",  # todo: applatir sur le dernier actif
        "parametres": "parametres",  # todo: à retirer
        "parametres_actifs": "parametres_actifs",  # todo: à retirer
        "parametres_actifs_pertinents": "parametres_actifs_pertinents",  # todo: à conserver, bien vérifier
    }

    # On sélectionne en appliquant le mapping dynamiquement
    _____df = ____df.select([pl.col(old).alias(new) for old, new in column_mapping.items()])
    return (_____df,)


@app.cell
def _(_____df, ____df):
    print(len(____df.columns), ____df.columns)
    print(len(_____df.columns), _____df.columns)
    return


@app.cell
def _(_____df):
    _____df
    return


@app.cell
def _(DATA_BRONZE, pl):
    # https://public-api.meteofrance.fr/public/DPPaquetObs/v1/liste-stations
    liste_stations_path = DATA_BRONZE / "liste-stations-obs-metropole-om.csv"
    df_from_api = pl.read_csv(
        liste_stations_path,
        separator=";",
        has_header=True,
        schema_overrides={"Id_station": pl.String},
    )

    print(df_from_api.columns)
    print(df_from_api.shape)
    return (df_from_api,)


@app.cell
def _(_____df, pl):
    _____df_seulement_type_0_a_2 = _____df.filter(
        pl.col("type_poste_actif")
        .list.eval(pl.element().struct.field("type").is_in([0, 1, 2]))
        .list.sum()
        > 0
    )
    return (_____df_seulement_type_0_a_2,)


@app.cell
def _(_____df_seulement_type_0_a_2, df_from_api):
    # vérifications avec l'api
    stations_pas_dans_le_referentiel = df_from_api.join(
        _____df_seulement_type_0_a_2, left_on="Id_station", right_on="id", how="anti"
    )

    print("stations_pas_dans_le_referentiel:", stations_pas_dans_le_referentiel.shape)
    stations_pas_dans_le_referentiel
    return


@app.cell
def _(_____df_seulement_type_0_a_2, df_from_api):
    stations_pas_dans_api = _____df_seulement_type_0_a_2.join(
        df_from_api, left_on="id", right_on="Id_station", how="anti"
    )

    print("stations_pas_dans_api:", stations_pas_dans_api.shape)
    stations_pas_dans_api
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("id") == "01384003")
    return


@app.cell
def _(_____df, pl):
    _____df_seulement_type_0 = _____df.filter(
        pl.col("type_poste_actif").list.eval(pl.element().struct.field("type") == 0).list.sum() > 0
    )
    _____df_seulement_type_0
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
