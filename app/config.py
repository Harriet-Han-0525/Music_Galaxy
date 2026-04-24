from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE_DIR / "final_dataset"

load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.example")


def get_settings() -> dict[str, str | int]:
    return {
        "host": os.getenv("PGHOST", "127.0.0.1"),
        "port": int(os.getenv("PGPORT", "5432")),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", ""),
        "admin_database": os.getenv("PGADMIN_DATABASE", "postgres"),
        "project_database": os.getenv("PROJECT_DATABASE", "music_galaxy"),
    }
