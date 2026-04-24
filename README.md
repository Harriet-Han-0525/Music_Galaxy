# Music Galaxy

Music Galaxy is a lightweight FastAPI + PostgreSQL project for the music relationship database assignment. It imports the curated CSV dataset, exposes relational data through APIs, and renders a web interface for overview browsing, SQL demonstrations, and a 3D graph view.

## What it includes

- PostgreSQL schema and CSV import script
- FastAPI backend with JSON endpoints
- English web UI for tracks, artists, SQL demos, and the 3D galaxy
- Graph coordinates derived from `valence`, `energy`, and `tempo`

## Important folders

- `app/`: Contains the main backend application code, including API routes, database access logic, and HTML templates.
- `app/templates/`: Contains the HTML templates for the web pages, such as the homepage, track and artist pages, SQL demo page, and 3D galaxy view.
- `app/static/`: Contains static frontend assets such as CSS styles.
- `final_dataset/`: Contains the curated CSV dataset used to initialize and populate the PostgreSQL database.

## Important files

- `app/main.py`: Defines the FastAPI application, page routes, and JSON API endpoints.
- `app/repository.py`: Implements the core database queries and relationship retrieval logic.
- `app/init_db.py`: Creates the database schema and imports the CSV dataset into PostgreSQL.
- `app/database.py`: Provides reusable database connection helpers.
- `app/config.py`: Loads environment variables and database configuration.
- `app/templates/base.html`: Defines the shared layout and navigation structure for all pages.
- `app/templates/index.html`: Implements the homepage of the project.
- `app/templates/tracks.html`: Implements the track list page.
- `app/templates/track_detail.html`: Implements the track detail page.
- `app/templates/artists.html`: Implements the artist list page.
- `app/templates/artist_detail.html`: Implements the artist detail page.
- `app/templates/sql_demo.html`: Implements the SQL demonstration page.
- `app/templates/galaxy.html`: Implements the interactive 3D galaxy visualization page.
- `app/static/styles.css`: Defines the main visual style of the web interface.
- `run.sh`: Initializes the database if needed and starts the project server.
- `music_galaxy_environment.yml`: Defines the Conda environment and project dependencies.

## Setup

1. Create the conda environment:

   ```bash
   conda env create -f music_galaxy_environment.yml
   conda activate music-galaxy

2. Start the project with the helper script:

```
bash run.sh
```

3. Open http://127.0.0.1:8000.

## Default PostgreSQL settings

- host: `127.0.0.1`
- port: `5432`
- user: `postgres`
- admin database for bootstrapping: `postgres`
- project database to create: `music_galaxy`
