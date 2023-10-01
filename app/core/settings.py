from pydantic import (
    Field
)

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    project_title: str = "Caris DATA API SERVICE"
    project_description: str = "REST API service for the datim reports and other relevant data analysis reports"
    project_version: str = "v0.0.1"
    project_docs_url: str = "/"

    AGYWPREVTABI_DESCRIPTION: str = "Get the first component / table for the dreams indicator - AGYW_PREV"
    AGYWPREVTABI_SUMMARY: str = "Get the first component / table for dreams indicator - AGYW_PREV"
    
    AGYWPREVTABII_DESCRIPTION: str = "Get the second component / table for the dreams indicator - AGYW_PREV"
    AGYWPREVTABII_SUMMARY: str = "Get the second component / table for dreams indicator - AGYW_PREV"
    
    AGYWPREVTABIII_DESCRIPTION: str = "Get the third component / table for the dreams indicator - AGYW_PREV"
    AGYWPREVTABIII_SUMMARY: str = "Get the third component / table for dreams indicator - AGYW_PREV"
    
    AGYWPREVTABIV_DESCRIPTION: str = "Get the third component / table for the dreams indicator - AGYW_PREV"
    AGYWPREVTABIV_SUMMARY: str = "Get the third component / table for dreams indicator - AGYW_PREV"

    VITAL_DESCRIPTION: str = "Get vital info link to the dreams indicator - AGYW_PREV"
    VITAL_SUMMARY: str = "Get vital info link to the dreams indicator - AGYW_PREV"

    DATIM_DESCRIPTION: str = "Get the datim data for dreams indicator - AGYW_PREV"
    DATIM_SUMMARY: str = "Get the datim data for dreams indicator - AGYW_PREV"
    
    mysql_username: str
    mysql_password: str 
    mysql_host: str 
    mysql_port: int 
    mysql_database: str 
    
    commcare_username: str 
    commcare_password: str 
    mongodb_uri: str 

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
settings = Settings()
