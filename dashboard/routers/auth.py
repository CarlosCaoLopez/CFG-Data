from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from dashboard.auth import check_password, make_token

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    return templates.TemplateResponse("login.html", {"request": request, "error": error})


@router.post("/auth/login")
async def do_login(request: Request, password: str = Form(...)):
    if check_password(password):
        token = make_token()
        response = RedirectResponse(url="/", status_code=302)
        # Session cookie: no max_age → deleted when browser closes
        response.set_cookie("ds_token", token, httponly=True, samesite="lax")
        return response
    return RedirectResponse(url="/login?error=1", status_code=302)


@router.get("/auth/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("ds_token")
    return response
