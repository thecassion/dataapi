from mangum import Mangum
from fastapi import FastAPI
from .core import (
    settings
)
from .config import create_celery

from .routers import  muso_router
from .routers import cases_router
from .routers import ptme_router


app = FastAPI(
    title=settings.project_title,
    description=settings.project_description,
    version=settings.project_version,
    docs_url=settings.project_docs_url
)


celery = create_celery() #TODO celery function is not yet implemented


app.include_router(muso_router)
app.include_router(cases_router)
app.include_router(ptme_router)



handler = Mangum(app=app)
