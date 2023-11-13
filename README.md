# usersProfileAnalysis_RealTimeData
# Kafka Spark Cassandra MongoDB Integration

## Description

Ce projet vise à démontrer comment intégrer les technologies Spark, Kafka, Cassandra et MongoDB pour traiter et stocker des données de profil utilisateur. Il ingère des données JSON à partir d'un topic Kafka, les traite à l'aide de Spark, stocke les résultats dans une base de données Cassandra, et enregistre également les données dans une base de données MongoDB pour la visualisation.

## Fonctionnalités

- Ingestion de données à partir d'un topic Kafka.
- Transformation des données JSON à l'aide de Spark.
- Stockage des données traitées dans Cassandra.
 <img width="850" alt="Capture d’écran 2023-11-02 144029" src="https://github.com/fatimazahrae-bh/usersProfileAnalysis_RealTimeData/assets/72580954/4d306fe4-6011-4af6-8792-4c89fccc99cf">

- Enregistrement des données dans MongoDB.
 <img width="580" alt="image" src="https://github.com/fatimazahrae-bh/usersProfileAnalysis_RealTimeData/assets/72580954/286e82b7-c334-4031-b5ec-397efdc02af3">

- Chiffrement de certaines données sensibles.

## Captures d'écran

_des captures d'écran_

## Installation

1. Assurez-vous que les dépendances Spark, Cassandra, MongoDB et Kafka sont correctement installées et configurées sur votre machine.
2. Exécutez le script `spark_app.py` à l'aide de Python.

## Utilisation

Pour utiliser ce projet, assurez-vous que les services Kafka, Cassandra et MongoDB sont en cours d'exécution sur vos serveurs locaux. Vous pouvez exécuter le script `spark_app.py` pour commencer l'ingestion, le traitement et le stockage des données.

## Technologies Utilisées
- Apache Spark
- Apache Kafka
- Apache Cassandra
- MongoDB
- Python

## Tableau de Bord Dash
<img width="880" alt="image" src="https://github.com/fatimazahrae-bh/usersProfileAnalysis_RealTimeData/assets/72580954/7ee8d220-b572-44ed-be0f-4b40690c3cd3">
<img width="889" alt="image" src="https://github.com/fatimazahrae-bh/usersProfileAnalysis_RealTimeData/assets/72580954/17417924-335e-406b-a97c-d5de36d45b01">


#  RGPD

## Respect de la protection des données

Ce projet est conforme au Règlement général sur la protection des données (RGPD) et met en œuvre des mesures de sécurité pour garantir la confidentialité et l'intégrité des données personnelles. Voici un résumé des principales considérations RGPD :

### Registre des traitements de données

Nous maintenons un registre détaillé de tous les traitements de données personnelles effectués dans ce projet. Ce registre comprend des informations sur les types de données stockées, les finalités du traitement, les durées de conservation, les bases légales du traitement et les mesures de sécurité mises en place.

### Consentement et transparence

Lorsque cela est nécessaire, nous obtenons un consentement explicite des utilisateurs pour collecter et traiter leurs données personnelles. Nous expliquons clairement les finalités du traitement des données et veillons à ce que les utilisateurs aient un contrôle sur leurs données.

### Sécurité des données

Nous mettons en place des mesures de sécurité robustes pour protéger les données personnelles contre tout accès non autorisé, perte ou divulgation. Cela comprend la gestion des accès, le chiffrement des données sensibles et la surveillance continue.

### Droits des personnes concernées

Nous respectons les droits des personnes concernées en ce qui concerne leurs données personnelles. Les utilisateurs ont le droit de demander l'accès à leurs données, de les rectifier, de les supprimer ou de s'opposer au traitement, conformément au RGPD.
