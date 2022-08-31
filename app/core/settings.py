from pydantic import (
    BaseSettings,
    Field
)

class Settings(BaseSettings):
    project_title: str = "Caris DATA API SERVICE"
    project_description: str = "REST API service for the datim reports and other relevant data analysis"
    project_version: str = "v0.0.1"
    project_docs_url: str = "/"
