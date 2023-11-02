import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pymongo

# Initialisation de l'application Dash
app = dash.Dash(__name__)

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["user_profiles"]
collection = db["users"]

# Définition de la mise en page du tableau de bord
app.layout = html.Div([
    dcc.Graph(id='my-graph'),
    dcc.Dropdown(id='data-dropdown',
                 options=[
                     {'label': 'Nombre total d'utilisateurs', 'value': 'total_users'},
                     {'label': 'Répartition par genre', 'value': 'gender_distribution'}
                 ],
                 value='total_users')
])

# Création de fonctions de mise à jour du graphique
@app.callback(
    Output('my-graph', 'figure'),
    Input('data-dropdown', 'value')
)
def update_graph(selected_data):
    if selected_data == 'total_users':
        # Agrégation pour obtenir le nombre total d'utilisateurs
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1}
                }
            }
        ]
        result = list(collection.aggregate(pipeline))
        total_users = result[0]["count"]

        return {
            'data': [{'x': ['Total Users'], 'y': [total_users], 'type': 'bar', 'name': 'Total Users'}],
            'layout': {
                'title': 'Nombre total d\'utilisateurs'
            }
        }
    elif selected_data == 'gender_distribution':
        # Agrégation pour obtenir la répartition par genre
        pipeline = [
            {
                "$group": {
                    "_id": "$gender",
                    "count": {"$sum": 1}
                }
            }
        ]
        result = list(collection.aggregate(pipeline))
        genders = [res["_id"] for res in result]
        counts = [res["count"] for res in result]

        return {
            'data': [{'x': genders, 'y': counts, 'type': 'bar', 'name': 'Genre Distribution'}],
            'layout': {
                'title': 'Répartition par genre'
            }
        }

if __name__ == '__main__':
    app.run_server(debug=True)
