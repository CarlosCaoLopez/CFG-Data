"""Dashboard FastAPI app — monitoring for the Polymarket insider detection system."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from pathlib import Path

from dashboard.routers import overview, markets, insiders, pipeline, trades, variables, sse, settings, auth as auth_router
from dashboard.auth import AuthMiddleware

app = FastAPI(title="Insider Detection Dashboard", docs_url="/api/docs")

app.add_middleware(AuthMiddleware)

# ── Static files (logos, favicons) ───────────────────────────────────────────
_public_dir = Path(__file__).parent.parent / "public"
app.mount("/static", StaticFiles(directory=str(_public_dir)), name="static")

# ── Routers ──────────────────────────────────────────────────────────────────
app.include_router(auth_router.router)
app.include_router(overview.router)
app.include_router(markets.router)
app.include_router(insiders.router)
app.include_router(pipeline.router)
app.include_router(trades.router)
app.include_router(variables.router)
app.include_router(sse.router)
app.include_router(settings.router)

# ── Templates ─────────────────────────────────────────────────────────────────
_templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_templates_dir))


# ── Page routes ───────────────────────────────────────────────────────────────
@app.get("/")
async def page_overview(request: Request):
    return templates.TemplateResponse("overview.html", {"request": request, "active": "overview"})


@app.get("/markets")
async def page_markets(request: Request):
    return templates.TemplateResponse("markets.html", {"request": request, "active": "markets"})


@app.get("/markets/{market_id}")
async def page_market_detail(request: Request, market_id: str):
    return templates.TemplateResponse("market_detail.html", {"request": request, "active": "markets", "market_id": market_id})


@app.get("/insiders")
async def page_insiders(request: Request):
    return templates.TemplateResponse("insiders.html", {"request": request, "active": "insiders"})


@app.get("/insiders/{wallet}")
async def page_insider_detail(request: Request, wallet: str):
    return templates.TemplateResponse("insider_detail.html", {"request": request, "active": "insiders", "wallet": wallet})


@app.get("/pipeline")
async def page_pipeline(request: Request):
    return templates.TemplateResponse("pipeline.html", {"request": request, "active": "pipeline"})


@app.get("/trades")
async def page_trades(request: Request):
    return templates.TemplateResponse("trades.html", {"request": request, "active": "trades"})


@app.get("/variables")
async def page_variables(request: Request):
    return templates.TemplateResponse("variables.html", {"request": request, "active": "variables"})


@app.get("/settings")
async def page_settings(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request, "active": "settings"})
