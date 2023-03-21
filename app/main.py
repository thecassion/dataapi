from mangum import Mangum
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .core import (
    settings
)
# from .config import create_celery

from .routers import  muso_router
from .routers import cases_router
from .routers import ptme_router
from .routers import dreams_router


app = FastAPI(
    title=settings.project_title,
    description=settings.project_description,
    version=settings.project_version,
    #docs_url=settings.project_docs_url
)


origins=[
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# celery = create_celery() #TODO celery function is not yet implemented


app.include_router(ptme_router)
app.include_router(cases_router)
app.include_router(dreams_router)
app.include_router(muso_router)


@app.get("/")
def home():
    return {
        "message": "The documentation is avalaible on the localhost:8000/docs"
    }


handler = Mangum(app=app)
