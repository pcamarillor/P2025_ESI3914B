from datetime import datetime
import random
import uuid

def generar_usuario_suscripcion():
    generos = ["Comedia", "Drama", "Acción", "Ciencia Ficción", "Terror", "Romance", "Animación", "Documental", "Thriller", "Fantasía", "Misterio", "Aventura", "Crimen", "Histórico", "Musical", "Western", "Bélico", "Deportivo", "Familiar", "Indie"]
    directores = [
        "Christopher Nolan", "Quentin Tarantino", "Greta Gerwig", "Bong Joon-ho", "Hayao Miyazaki",
        "Denis Villeneuve", "Taika Waititi", "Jordan Peele", "Steven Spielberg", "Alfonso Cuarón",
        "Martin Scorsese", "Spike Lee", "Kathryn Bigelow", "Paul Thomas Anderson", "Wes Anderson",
        "Jane Campion", "Chloé Zhao", "Guillermo del Toro", "Pedro Almodóvar", "Park Chan-wook",
        "Francis Ford Coppola", "Stanley Kubrick", "Akira Kurosawa", "Ingmar Bergman", "Federico Fellini",
        "David Lynch", "Ridley Scott", "James Cameron", "George Miller", "Sofia Coppola"
    ]
    decadas = ["1970", "1980", "1990", "2000", "2010", "2020"]
    clasificaciones = ["G", "PG", "PG-13", "R", "NC-17"]
    actores = [
        "Tom Hanks", "Scarlett Johansson", "Leonardo DiCaprio", "Meryl Streep", "Denzel Washington",
        "Cate Blanchett", "Joaquin Phoenix", "Saoirse Ronan", "Ryan Gosling", "Emma Stone",
        "Brad Pitt", "Angelina Jolie", "Robert Downey Jr.", "Natalie Portman", "Christian Bale",
        "Viola Davis", "Mahershala Ali", "Lupita Nyong'o", "Timothée Chalamet", "Florence Pugh",
        "Idris Elba", "Naomi Watts", "Javier Bardem", "Penélope Cruz", "Anthony Hopkins",
        "Julianne Moore", "Ethan Hawke", "Uma Thurman", "Keanu Reeves", "Charlize Theron"
    ]

    # Generar un nombre de usuario único
    nombre_usuario = f"usuario_{uuid.uuid4().hex[:8]}"

    # Generar un ID único
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    random_unique = uuid.uuid4().hex[:6]
    id_usuario = f"{timestamp}_{random_unique}"

    return {
        'id': id_usuario,
        'nombre_usuario': nombre_usuario,
        'generos_favoritos': random.sample(generos, random.randint(1, 3)),
        'directores_favoritos': random.sample(directores, random.randint(1, 3)),
        'decadas_favoritas': random.sample(decadas, random.randint(1, 2)),
        'clasificacion_favorita': random.choice(clasificaciones),
        'actores_favoritos': random.sample(actores, random.randint(2, 4)),
        'fecha_suscripcion': datetime.now().strftime('%Y-%m-%d')
    }