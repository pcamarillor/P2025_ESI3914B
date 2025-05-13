from notebooks.final_project.TeamBellaco.libs import title_gen
import random
import time
           
MAX_MOVIE_ID = 2000                
MIN_RATING = 1                     
MAX_RATING = 10                     
MOVIE_TITLES_STORAGE_FILE = "movie_id_to_title.txt"

def get_or_generate_movie_name_from_file(movie_id, filename):
  
    movie_id_str = str(movie_id)
    
    current_movie_map_from_file = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
            
                parts = line.split('|', 1)
                if len(parts) == 2:
                    stored_id, stored_name = parts[0].strip(), parts[1].strip()
                    current_movie_map_from_file[stored_id] = stored_name
                else:
                    print(f"Warning: Malformed line in '{filename}': {line}")
    except FileNotFoundError:
        pass
        
    if movie_id_str in current_movie_map_from_file:
        return current_movie_map_from_file[movie_id_str]
    else:
        
        new_base_name = title_gen.generate_random_title()
        
        
        try:
            
            with open(filename, 'a') as f:
                f.write(f"{movie_id_str}|{new_base_name}\n")
        except IOError as e:
            print(f"Error: Could not write to file '{filename}': {e}")
            
        return new_base_name

SAMPLE_GENRES = [
    "Action", "Comedy", "Drama", "Sci-Fi", "Fantasy", "Thriller",
    "Romance", "Horror", "Documentary", "Adventure", "Mystery"
]
        
def generate_movie_rating_data(user_id, rated_movie_ids_for_user):
    
    movie_id = random.randint(1, MAX_MOVIE_ID)
    while movie_id in rated_movie_ids_for_user:
        movie_id = random.randint(1, MAX_MOVIE_ID)
    
    rating = random.randint(MIN_RATING, MAX_RATING)
    movie_title = get_or_generate_movie_name_from_file( movie_id , MOVIE_TITLES_STORAGE_FILE )
    genre = random.choice(SAMPLE_GENRES)
    
    return {
        'userId': user_id,
        'movieId': movie_id,
        'movieTitle': movie_title, 
        'genre': genre,
        'rating': rating,
        'timestamp': int(time.time() * 1000) 
    }
    