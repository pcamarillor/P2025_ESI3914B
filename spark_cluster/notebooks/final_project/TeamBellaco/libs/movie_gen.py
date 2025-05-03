import random
import uuid

def generar_pelicula():
    generos = ["Comedia", "Drama", "Acción", "Ciencia Ficción", "Terror", "Romance", "Animación", "Documental", "Thriller", "Fantasía", "Misterio", "Aventura", "Crimen", "Histórico", "Musical", "Western", "Bélico", "Deportivo", "Familiar", "Indie"]
    calidades = ["HD", "Full HD", "4K"]
    ratings_imdb = [str(round(random.uniform(1.0, 10.0), 1)) for _ in range(20)] # Genera algunos ratings aleatorios
    duraciones = [f"{random.randint(60, 240)} minutos" for _ in range(15)] # Genera algunas duraciones aleatorias
    decadas = ["1970", "1980", "1990", "2000", "2010", "2020"]
    ratings_edad = ["G", "PG", "PG-13", "R", "NC-17"]
    directores = [
        "Christopher Nolan", "Quentin Tarantino", "Greta Gerwig", "Bong Joon-ho", "Hayao Miyazaki",
        "Denis Villeneuve", "Taika Waititi", "Jordan Peele", "Steven Spielberg", "Alfonso Cuarón",
        "Martin Scorsese", "Spike Lee", "Kathryn Bigelow", "Paul Thomas Anderson", "Wes Anderson",
        "Jane Campion", "Chloé Zhao", "Guillermo del Toro", "Pedro Almodóvar", "Park Chan-wook",
        "Francis Ford Coppola", "Stanley Kubrick", "Akira Kurosawa", "Ingmar Bergman", "Federico Fellini",
        "David Lynch", "Ridley Scott", "James Cameron", "George Miller", "Sofia Coppola"
    ]
    actores = [
        "Tom Hanks", "Scarlett Johansson", "Leonardo DiCaprio", "Meryl Streep", "Denzel Washington",
        "Cate Blanchett", "Joaquin Phoenix", "Saoirse Ronan", "Ryan Gosling", "Emma Stone",
        "Brad Pitt", "Angelina Jolie", "Robert Downey Jr.", "Natalie Portman", "Christian Bale",
        "Viola Davis", "Mahershala Ali", "Lupita Nyong'o", "Timothée Chalamet", "Florence Pugh",
        "Idris Elba", "Naomi Watts", "Javier Bardem", "Penélope Cruz", "Anthony Hopkins",
        "Julianne Moore", "Ethan Hawke", "Uma Thurman", "Keanu Reeves", "Charlize Theron"
    ]
    titulos_parte1 = ["La", "El", "Los", "Las", "Una", "Un"]
    titulos_parte2 = ["Aventura", "Misterio", "Venganza", "Sombra", "Secreto", "Legado", "Búsqueda", "Escape", "Caída", "Ascenso"]
    titulos_parte3 = ["de Fuego", "en la Noche", "del Tiempo", "Olvidada", "Imposible", "Final", "Oscura", "Perdida", "Eterna", "Silenciosa"]

    
    id_pelicula = uuid.uuid4().hex

    
    titulo = f"{random.choice(titulos_parte1)} {random.choice(titulos_parte2)} {random.choice(titulos_parte3)}"

    return {
        'id_pelicula': id_pelicula,
        'titulo': titulo,
        'genero': random.choice(generos),
        'calidad': random.choice(calidades),
        'rating': random.choice(ratings_imdb),
        'duracion': random.choice(duraciones),
        'decada': random.choice(decadas),
        'rating_edad': random.choice(ratings_edad),
        'director': random.choice(directores),
        'actores': random.sample(actores, random.randint(1, 4))
    }
