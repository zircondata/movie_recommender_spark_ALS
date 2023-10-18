from flask import Flask, Blueprint, render_template, jsonify, request
import json
import findspark
from engine import RecommendationEngine

main = Blueprint("main", __name__)  # Création d'un Blueprint "main"
findspark.init()                    # Initialisation de SparkContext

@main.route("/", methods=["GET", "POST", "PUT"])
def home():
    return render_template("index.html")

@main.route("/movies/<int:movie_id>", methods=["GET"])      # Route pour obtenir les détails d'un film par son ID
def get_movie(movie_id):
    movie_details = recommender.get_movie(movie_id)                                                     # Récupérer les détails du film avec l'ID spécifié
    if movie_details is not None:                                                                       # Vérifier si le film existe
        movie_details_json = movie_details.toJSON().collect()                                           # Convertir le DataFrame en un format JSON compatible
        return json.dumps(movie_details_json), 200, {'Content-Type': 'application/json'}
    else:
        return json.dumps({"message": "Film non trouvé"}), 404, {'Content-Type': 'application/json'}    # Si le film n'existe pas, retourner une réponse 404
    
@main.route("/newratings/<int:user_id>", methods=["POST"])  # Route pour ajouter de nouvelles évaluations pour les films
def new_ratings(user_id):
    
    if not recommender.is_user_known(user_id):              # Vérifier si l'utilisateur existe déjà 
        user_id = recommender.create_user(user_id)          # Si l'utilisateur n'existe pas, créez-le
    ratings = request.get_json()                            # Récupérer les évaluations depuis la requête
    recommender.add_ratings(user_id, ratings)               # Ajouter les évaluations au moteur de recommandation
    if not recommender.is_user_known(user_id):              # Renvoyer l'identifiant de l'utilisateur s'il est nouveau, sinon renvoyer une chaîne vide
        return str(user_id)
    else:
        return ""

@main.route("/<int:user_id>/ratings", methods=["POST"])     # Route pour ajouter des évaluations à partir d'un fichier
def add_ratings(user_id):
    uploaded_file = request.files['file']                   # Code pour récupérer le fichier téléchargé depuis la requête
    if uploaded_file.filename != '':
        ratings = []                                        # Lire les données du fichier et ajouter-les au moteur de recommandation
        with open(uploaded_file.filename, 'r') as file:
            for line in file:
                movie_id, rating = line.strip().split(',')
                ratings.append((int(movie_id), float(rating)))
        recommender.add_ratings(user_id, ratings)
        return "Le modèle de prédiction a été recalculé."   # Renvoyer un message indiquant que le modèle de prédiction a été recalculé
    else:
        return "Aucun fichier téléchargé."

@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])                        # Route pour obtenir la note prédite d'un utilisateur pour un film
def movie_ratings(user_id, movie_id):
    predicted_rating = recommender.predict_rating(user_id, movie_id)                         # Prédire la note de l'utilisateur pour le film spécifié
    if predicted_rating != -1:                                                               # Vérifier si une prédiction a pu être faite
        return f"La note prédite pour le film {movie_id} est : {predicted_rating}"           # Si oui, retourner la note prédite au format texte
    else:
        return "Impossible de faire une prédiction pour cette combinaison utilisateur/film." # Sinon, retourner un message indiquant qu'aucune prédiction n'a pu être faite

@main.route("/<int:user_id>/recommendations/<int:nb_movies>", methods=["GET"])               # Route pour obtenir les meilleures évaluations recommandées pour un utilisateur
def get_recommendations(user_id, nb_movies):
    recommended_movies = recommender.recommend_for_user(user_id, nb_movies)                  # Obtenir les meilleures recommandations pour l'utilisateur donné
    recommendations_json = recommended_movies.toJSON().collect()                             # Convertir les recommandations en format JSON
    return json.dumps(recommendations_json), 200, {'Content-Type': 'application/json'}

@main.route("/ratings/<int:user_id>", methods=["GET"])                                       # Route pour obtenir les évaluations d'un utilisateur
def get_ratings_for_user(user_id):
    user_ratings = recommender.get_ratings_for_user(user_id)                                 # Obtenir les évaluations de l'utilisateur spécifié
    ratings_json = user_ratings.toJSON().collect()                                           # Convertir les évaluations en format JSON
    return json.dumps(ratings_json), 200, {'Content-Type': 'application/json'}

def create_app(sc, movies_set_path, ratings_set_path):                                       # Fonction pour créer l'application Flask
    global recommender
    recommender = RecommendationEngine(sc, movies_set_path, ratings_set_path)       # Initialiser le moteur de recommandation avec le contexte Spark et les jeux de données
    app = Flask(__name__) 
    app.register_blueprint(main)    # Enregistrez le Blueprint "main" dans l'application
                                    # Configurer les options de l'application Flask
                                    # app.config['SECRET_KEY'] = 'ahmed'
                                    # Configurez les options de l'application Flask (si nécessaire)
                                    # app.config['OPTION_NAME'] = 'OPTION_VALUE'
                                    # Renvoyez l'application Flask créée
    return app