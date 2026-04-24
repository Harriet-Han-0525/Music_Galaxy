from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.repository import (
    build_graph_payload,
    create_sampling_relation,
    create_track_with_relations,
    fetch_overview,
    fetch_reference_data,
    get_artist_detail,
    get_cover_family_subgraph,
    get_sampling_trace_subgraph,
    get_track_detail,
    list_artists,
    list_tracks,
    run_sql_demos,
)


class SamplingRelationCreate(BaseModel):
    sampling_relation_id: str | None = None
    sampling_track_id: str = Field(..., min_length=1)
    sampled_track_id: str = Field(..., min_length=1)
    relation_type: str | None = None
    verification_level: str | None = None
    note: str | None = None


class TrackArtistInput(BaseModel):
    artist_id: str
    artist_role: str
    credit_order: int | None = None


class TrackAudioFeaturesInput(BaseModel):
    tempo: float | None = None
    energy: float | None = None
    valence: float | None = None
    danceability: float | None = None
    acousticness: float | None = None
    speechiness: float | None = None
    liveness: float | None = None
    loudness: float | None = None
    instrumentalness: float | None = None


class TrackCreate(BaseModel):
    track_id: str | None = None
    track_name: str
    album_id: str | None = None
    release_date: str | None = None
    duration_ms: int | None = None
    popularity: int | None = None
    external_source: str | None = None
    external_track_ref: str | None = None
    features: TrackAudioFeaturesInput | None = None
    artists: list[TrackArtistInput]


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="Music Galaxy", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def template_context(request: Request, page_title: str, active_page: str) -> dict:
    return {
        "request": request,
        "page_title": page_title,
        "active_page": active_page,
    }


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if request.url.path.startswith("/api/"):
        return JSONResponse(
            status_code=500,
            content={"error": str(exc), "hint": "Run the database initialization script first."},
        )
    raise exc


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        template_context(request, "Overview", "overview"),
    )


@app.get("/tracks", response_class=HTMLResponse)
def tracks_page(request: Request):
    return templates.TemplateResponse(
        request,
        "tracks.html",
        template_context(request, "Tracks", "tracks"),
    )


@app.get("/tracks/{track_id}", response_class=HTMLResponse)
def track_detail_page(request: Request, track_id: str):
    detail = get_track_detail(track_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Track not found")
    context = template_context(request, detail["track"]["track_name"], "tracks")
    context["detail"] = detail
    return templates.TemplateResponse(request, "track_detail.html", context)


@app.get("/artists", response_class=HTMLResponse)
def artists_page(request: Request):
    return templates.TemplateResponse(
        request,
        "artists.html",
        template_context(request, "Artists", "artists"),
    )


@app.get("/artists/{artist_id}", response_class=HTMLResponse)
def artist_detail_page(request: Request, artist_id: str):
    detail = get_artist_detail(artist_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Artist not found")
    context = template_context(request, detail["artist"]["artist_name"], "artists")
    context["detail"] = detail
    return templates.TemplateResponse(request, "artist_detail.html", context)


@app.get("/sql-demo", response_class=HTMLResponse)
def sql_demo_page(request: Request):
    context = template_context(request, "SQL Demo", "sql-demo")
    context["demos"] = run_sql_demos()
    return templates.TemplateResponse(request, "sql_demo.html", context)


@app.get("/galaxy", response_class=HTMLResponse)
def galaxy_page(request: Request):
    context = template_context(request, "Music Galaxy", "galaxy")
    context["body_class"] = "galaxy-immersive-page"
    return templates.TemplateResponse(request, "galaxy.html", context)


@app.get("/api/overview")
def overview_api():
    return fetch_overview()


@app.get("/api/tracks")
def tracks_api(q: str = Query(default="", max_length=100), limit: int = Query(default=50, le=200)):
    return {"items": list_tracks(search=q, limit=limit)}


@app.get("/api/tracks/{track_id}")
def track_detail_api(track_id: str):
    detail = get_track_detail(track_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Track not found")
    return detail


@app.post("/api/tracks")
def create_track_api(payload: TrackCreate):
    try:
        created = create_track_with_relations(payload.model_dump())
        return created
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/artists")
def artists_api(q: str = Query(default="", max_length=100), limit: int = Query(default=50, le=200)):
    return {"items": list_artists(search=q, limit=limit)}


@app.get("/api/artists/{artist_id}")
def artist_detail_api(artist_id: str):
    detail = get_artist_detail(artist_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Artist not found")
    return detail


@app.get("/api/sql-demos")
def sql_demos_api():
    return {"items": run_sql_demos()}


@app.get("/api/reference-data")
def reference_data_api():
    return fetch_reference_data()


@app.get("/api/graph")
def graph_api():
    return build_graph_payload()


@app.get("/api/tracks/{track_id}/sampling-trace")
def sampling_trace_api(track_id: str):
    try:
        return get_sampling_trace_subgraph(track_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message)


@app.get("/api/tracks/{track_id}/cover-family")
def cover_family_api(track_id: str):
    try:
        return get_cover_family_subgraph(track_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message)


@app.post("/api/sampling-relations")
def create_sampling_relation_api(payload: SamplingRelationCreate):
    try:
        created = create_sampling_relation(payload.model_dump())
        return {"item": created}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))