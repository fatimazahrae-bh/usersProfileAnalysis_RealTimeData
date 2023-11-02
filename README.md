# usersProfileAnalysis_RealTimeData
# Kafka Spark Cassandra MongoDB Integration

## Description

Ce projet vise à démontrer comment intégrer les technologies Spark, Kafka, Cassandra et MongoDB pour traiter et stocker des données de profil utilisateur. Il ingère des données JSON à partir d'un topic Kafka, les traite à l'aide de Spark, stocke les résultats dans une base de données Cassandra, et enregistre également les données dans une base de données MongoDB pour la visualisation.

## Fonctionnalités

- Ingestion de données à partir d'un topic Kafka.
- Transformation des données JSON à l'aide de Spark.
- Stockage des données traitées dans Cassandra.
- Enregistrement des données dans MongoDB.
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

