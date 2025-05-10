import random

# --- Word Lists for Title Generation ---

EPIC_ADJECTIVES = [
    "Eternal", "Forgotten", "Lost", "Golden", "Crystal", "Shadow", "Crimson", "Frozen",
    "Silent", "Hidden", "Ancient", "Mystic", "Dark", "Bright", "Brave", "Last",
    "Final", "Forbidden", "Sacred", "Cosmic", "Galactic", "Iron", "Steel", "Solar",
    "Lunar", "Phantom", "Invisible", "Savage", "Noble", "Wicked", "Divine"
]

EPIC_NOUNS = [
    "Kingdom", "Empire", "Treasure", "Secrets", "Legends", "Prophecy", "Destiny", "Chronicles",
    "Saga", "Quest", "Curse", "Blade", "Throne", "Crown", "Scrolls", "Orb", "Talisman",
    "Fortress", "Citadel", "Sanctuary", "Island", "Mountain", "River", "Forest", "Desert",
    "Ocean", "Star", "Void", "Nexus", "Guardian", "Warrior", "Sorcerer", "Dragon", "Phoenix",
    "Wolf", "Serpent", "Ghost", "Revenant", "Knight", "King", "Queen", "Prince", "Princess"
]

MYSTERY_THRILLER_ADJECTIVES = [
    "Missing", "Silent", "Dark", "Twisted", "Hidden", "Forgotten", "Sinister", "Fatal",
    "Perfect", "Unknown", "Unsolved", "Broken", "Cold", "Deadly", "Secret", "Glass",
    "Strange", "Deceptive", "Shallow", "Scarlet", "Velvet", "Midnight"
]

MYSTERY_THRILLER_NOUNS = [
    "Witness", "Truth", "Lies", "Game", "Pact", "Guest", "House", "Room", "Window", "Key",
    "Code", "Cipher", "Agenda", "Motives", "Echo", "Shadow", "Figure", "Stranger",
    "Victim", "Suspect", "Alibi", "Diary", "Letter", "Photograph", "Obsession", "Fear"
]

SCIFI_FANTASY_ADJECTIVES = [
    "Cybernetic", "Galactic", "Quantum", "Chrono", "Astro", "Bio", "Robotic", "Android",
    "Mutant", "Alien", "Dimensional", "Stellar", "Nebula", "Terra", "Martian", "Venusian",
    "Future", "Past", "Parallel", "Virtual", "Arcane", "Enchanted", "Mythic"
]

SCIFI_FANTASY_NOUNS = [
    "Protocol", "Paradox", "Horizon", "Odyssey", "Colony", "Uprising", "Syndrome", "Experiment",
    "Matrix", "Nebula", "Galaxy", "Universe", "Spaceship", "Portal", "Dimension", "Artifact",
    "Creature", "Spell", "Warlock", "Elf", "Dwarf", "Oracle", "Golem", "Chimera", "Machine",
    "AI", "Network", "Planet", "Moon", "Comet", "Asteroid", "Anomaly", "Singularity"
]

ACTION_VERBS_GERUND = [
    "Chasing", "Hunting", "Breaking", "Falling", "Rising", "Running", "Searching", "Escaping",
    "Fighting", "Decoding", "Stealing", "Saving", "Building", "Crossing", "Entering",
    "Surviving", "Exploring", "Conquering", "Defending", "Avenging", "Remembering"
]

PLACES_PROPER_NOUNS = [
    "Earth", "Mars", "Xylos", "Cygnus X-1", "Alpha Centauri", "Andromeda", "Orion", "Sirius",
    "Avalon", "Camelot", "El Dorado", "Atlantis", "Olympus", "Valhalla", "Pandora",
    "Arkham", "Gotham", "Metropolis", "Zion", "Neptune", "Pluto", "Europa", "Titan",
    "Blackwood Manor", "Raven's Peak", "Willow Creek", "Serpent's Coil", "Dragon's Tooth"
]

COLORS = [
    "Red", "Blue", "Green", "Black", "White", "Golden", "Silver", "Crimson", "Emerald",
    "Sapphire", "Ruby", "Obsidian", "Ivory", "Scarlet", "Violet", "Azure"
]

ABSTRACT_CONCEPTS = [
    "Hope", "Despair", "Redemption", "Retribution", "Silence", "Madness", "Justice",
    "Freedom", "Chaos", "Order", "Beginning", "End", "Dawn", "Dusk", "Eternity", "Oblivion"
]

TITLE_TEMPLATES = [
    "The {epic_adj} {epic_noun}",
    "{epic_noun} of the {epic_adj} {epic_noun_alt}",
    "{action_verb} the {epic_noun}",
    "The {mystery_adj} {mystery_noun}",
    "{mystery_noun}: A {mystery_adj} Game",
    "Secrets of {place_name}",
    "The {sff_adj} {sff_noun}",
    "{sff_noun} from {place_name}",
    "Curse of the {epic_adj} {epic_noun}",
    "Legend of the {epic_noun}",
    "Chronicles of {place_name}",
    "The {color} {epic_noun}",
    "Escape from {place_name}",
    "{epic_adj} {abstract_concept}",
    "{abstract_concept} Falls",
    "{place_name}: {epic_noun} Rising",
    "Beneath the {epic_adj} {epic_noun}",
    "Beyond {place_name}",
    "The Last {epic_noun} of {place_name}",
    "{sff_noun}: {sff_adj} Protocol",
    "Project: {sff_noun}",
    "The {mystery_adj} Case of the {mystery_noun}",
    "Echoes of {abstract_concept}",
    "The {epic_noun}'s Gambit",
    "When the {epic_noun} {simple_verb_plural}", # Would need verbs in simple form
    "A Whisper in the {epic_adj} {epic_noun}"
]

# Simple verbs (for the template that needs them) - you can expand this list
SIMPLE_VERBS_PLURAL = [
    "Fall", "Rise", "Scream", "Whisper", "Bleed", "Burn", "Awaken", "Sleep", "Dream", "Die", "Live"
]


def generate_random_title():
    """Generates a single random movie title."""
    template = random.choice(TITLE_TEMPLATES)

    replacements = {
        "{epic_adj}": random.choice(EPIC_ADJECTIVES),
        "{epic_noun}": random.choice(EPIC_NOUNS),
        "{epic_noun_alt}": random.choice(EPIC_NOUNS), # To avoid repetition in the same template
        "{action_verb}": random.choice(ACTION_VERBS_GERUND),
        "{mystery_adj}": random.choice(MYSTERY_THRILLER_ADJECTIVES),
        "{mystery_noun}": random.choice(MYSTERY_THRILLER_NOUNS),
        "{place_name}": random.choice(PLACES_PROPER_NOUNS),
        "{sff_adj}": random.choice(SCIFI_FANTASY_ADJECTIVES),
        "{sff_noun}": random.choice(SCIFI_FANTASY_NOUNS),
        "{color}": random.choice(COLORS),
        "{abstract_concept}": random.choice(ABSTRACT_CONCEPTS),
        "{simple_verb_plural}": random.choice(SIMPLE_VERBS_PLURAL)
    }

    generated_title = template
    # Initial replacement pass
    for placeholder, word in replacements.items():
        if placeholder in generated_title:
            generated_title = generated_title.replace(placeholder, word, 1)

    for placeholder_key in replacements.keys(): # Iterate through known placeholders
        while placeholder_key in generated_title: # Keep replacing if it still exists
             # Dynamically get the correct list of words for the placeholder
            word_list_name = placeholder_key.strip("{}").upper()
            if "EPIC_ADJ" in word_list_name: word_list_to_use = EPIC_ADJECTIVES
            elif "EPIC_NOUN_ALT" in word_list_name: word_list_to_use = EPIC_NOUNS # Use same list for alt
            elif "EPIC_NOUN" in word_list_name: word_list_to_use = EPIC_NOUNS
            elif "ACTION_VERB" in word_list_name: word_list_to_use = ACTION_VERBS_GERUND
            elif "MYSTERY_ADJ" in word_list_name: word_list_to_use = MYSTERY_THRILLER_ADJECTIVES
            elif "MYSTERY_NOUN" in word_list_name: word_list_to_use = MYSTERY_THRILLER_NOUNS
            elif "PLACE_NAME" in word_list_name: word_list_to_use = PLACES_PROPER_NOUNS
            elif "SFF_ADJ" in word_list_name: word_list_to_use = SCIFI_FANTASY_ADJECTIVES
            elif "SFF_NOUN" in word_list_name: word_list_to_use = SCIFI_FANTASY_NOUNS
            elif "COLOR" in word_list_name: word_list_to_use = COLORS
            elif "ABSTRACT_CONCEPT" in word_list_name: word_list_to_use = ABSTRACT_CONCEPTS
            elif "SIMPLE_VERB_PLURAL" in word_list_name: word_list_to_use = SIMPLE_VERBS_PLURAL
            else: # Fallback or error
                print(f"Warning: Unknown placeholder key component: {placeholder_key}")
                break 
            generated_title = generated_title.replace(placeholder_key, random.choice(word_list_to_use), 1)


    return generated_title.title() # Capitalizes the first letter of each word



