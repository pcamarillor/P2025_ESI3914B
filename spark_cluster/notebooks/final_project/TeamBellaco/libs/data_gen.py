from datetime import datetime
import random
import uuid

# --- Shared Data Lists ---
GENEROS = ["Comedia", "Drama", "Acción", "Ciencia Ficción", "Terror", "Romance", "Animación", "Documental", "Thriller", "Fantasía", "Misterio", "Aventura", "Crimen", "Histórico", "Musical", "Western", "Bélico", "Deportivo", "Familiar", "Indie"]
DIRECTORES = [
    "Christopher Nolan", "Quentin Tarantino", "Greta Gerwig", "Bong Joon-ho", "Hayao Miyazaki",
    "Denis Villeneuve", "Taika Waititi", "Jordan Peele", "Steven Spielberg", "Alfonso Cuarón",
    "Martin Scorsese", "Spike Lee", "Kathryn Bigelow", "Paul Thomas Anderson", "Wes Anderson",
    "Jane Campion", "Chloé Zhao", "Guillermo del Toro", "Pedro Almodóvar", "Park Chan-wook",
    "Francis Ford Coppola", "Stanley Kubrick", "Akira Kurosawa", "Ingmar Bergman", "Federico Fellini",
    "David Lynch", "Ridley Scott", "James Cameron", "George Miller", "Sofia Coppola"
]
ACTORES = [
    "Tom Hanks", "Scarlett Johansson", "Leonardo DiCaprio", "Meryl Streep", "Denzel Washington",
    "Cate Blanchett", "Joaquin Phoenix", "Saoirse Ronan", "Ryan Gosling", "Emma Stone",
    "Brad Pitt", "Angelina Jolie", "Robert Downey Jr.", "Natalie Portman", "Christian Bale",
    "Viola Davis", "Mahershala Ali", "Lupita Nyong'o", "Timothée Chalamet", "Florence Pugh",
    "Idris Elba", "Naomi Watts", "Javier Bardem", "Penélope Cruz", "Anthony Hopkins",
    "Julianne Moore", "Ethan Hawke", "Uma Thurman", "Keanu Reeves", "Charlize Theron"
]
CLASIFICACIONES = ["G", "PG", "PG-13", "R", "NC-17", "TV-Y", "TV-G", "TV-PG", "TV-14", "TV-MA"]
DECADAS = ["1970", "1980", "1990", "2000", "2010", "2020"]

# --- Helper Function for Unique ID ---
def generate_unique_id(prefix="item"):
    """Generates a unique ID using timestamp and UUID."""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f') # Added microseconds for more uniqueness
    random_unique = uuid.uuid4().hex[:8]
    return f"{prefix}_{timestamp}_{random_unique}"

# --- Producer Data Generation Functions ---

def generar_datos_usuario():
    """Generates data for a user record."""
    user_id = generate_unique_id("user")
    username = f"usuario_{uuid.uuid4().hex[:8]}"
    now_iso = datetime.now().isoformat()

    return {
        'id': user_id,
        'type': 'user', # Identifier for the record type
        'timestamp': now_iso, # Timestamp of event generation
        'nombre_usuario': username,
        'fecha_suscripcion': datetime.now().strftime('%Y-%m-%d'),
        'generos': random.sample(GENEROS, random.randint(1, 3)), # Renamed from _favoritos for consistency
        'directores': random.sample(DIRECTORES, random.randint(1, 3)), # Renamed
        'actores': random.sample(ACTORES, random.randint(2, 4)), # Renamed
        'clasificacion': random.choice(CLASIFICACIONES), # Renamed
        'decadas_favoritas': random.sample(DECADAS, random.randint(1, 2)), # User specific preference
        # --- Fields not applicable to users ---
        'titulo': 'n/a',
        'decada_lanzamiento': 'n/a',
        'numero_temporadas': 'n/a',
        'numero_episodios': 'n/a',
    }

def generar_datos_pelicula():
    """Generates data for a movie record."""
    movie_id = generate_unique_id("movie")
    now_iso = datetime.now().isoformat()

    return {
        'id': movie_id,
        'type': 'movie', # Identifier for the record type
        'timestamp': now_iso, # Timestamp of event generation
        'titulo': f"Pelicula_{uuid.uuid4().hex[:6]}",
        'generos': random.sample(GENEROS, random.randint(1, 3)),
        'directores': random.sample(DIRECTORES, random.randint(1, 2)), # Usually 1, but can have co-directors
        'actores': random.sample(ACTORES, random.randint(3, 8)),
        'clasificacion': random.choice(CLASIFICACIONES),
        'decada_lanzamiento': random.choice(DECADAS),
        # --- Fields not applicable to movies ---
        'nombre_usuario': 'n/a',
        'fecha_suscripcion': 'n/a',
        'decadas_favoritas': 'n/a', # This field is for user preference
        'numero_temporadas': 'n/a',
        'numero_episodios': 'n/a',
    }

def generar_datos_serie():
    """Generates data for a series record."""
    series_id = generate_unique_id("series")
    num_temporadas = random.randint(1, 10)
    now_iso = datetime.now().isoformat()

    return {
        'id': series_id,
        'type': 'series', # Identifier for the record type
        'timestamp': now_iso, # Timestamp of event generation
        'titulo': f"Serie_{uuid.uuid4().hex[:6]}",
        'generos': random.sample(GENEROS, random.randint(1, 3)),
        'directores': random.sample(DIRECTORES, random.randint(1, 4)), # Series can have multiple directors
        'actores': random.sample(ACTORES, random.randint(5, 15)), # Series often have larger casts
        'clasificacion': random.choice(CLASIFICACIONES),
        'decada_lanzamiento': random.choice(DECADAS), # Could represent the decade it started
        'numero_temporadas': num_temporadas,
        'numero_episodios': random.randint(num_temporadas * 6, num_temporadas * 24), # Estimate episodes based on seasons
         # --- Fields not applicable to series ---
        'nombre_usuario': 'n/a',
        'fecha_suscripcion': 'n/a',
        'decadas_favoritas': 'n/a', # This field is for user preference
    }
