from pydantic import (
    BaseSettings,
    Field
)

class Settings(BaseSettings):
    project_title: str = "Caris DATA API SERVICE"
    project_description: str = "REST API service for the datim reports and other relevant data analysis reports"
    project_version: str = "v0.0.1"
    project_docs_url: str = "/"
    
    DATIM_DESCRIPTION: str = "Get the datim data for dreams indicator - AGYW_PREV"
    DATIM_SUMMARY: str = "Get the datim data for dreams indicator - AGYW_PREV"

    DISTRICT_DESCRIPTION: str = "Get the district data for dreams indicator - AGYW_PREV"
    DISTRICT_SUMMARY: str = "Get the district data for dreams indicator - AGYW_PREV"
    
    
    mysql_username: str = Field(...,env='MYSQL_USERNAME')
    mysql_password: str = Field(...,env='MYSQL_PASSWORD')
    mysql_host: str = Field(...,env='MYSQL_HOST')
    mysql_port: int = Field(...,env='MYSQL_PORT')
    mysql_database: str = Field(...,env='MYSQL_DATABASE')
    
    commcare_username: str = Field(...,env='COMMCARE_USERNAME')
    commcare_password: str = Field(...,env='COMMCARE_PASSWORD')
    
    class Config:
        env_prefix = ""
        case_sensitive = False
        env_file = ".env"
        env_file_encoding = "utf-8"
        
        
        
settings = Settings()
