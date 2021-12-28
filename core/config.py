

class Settings:
    PROJECT_TITLE:  str = "UNOPS DATA INTEGRATION"
    PORJECT_DESCRIPTION: str = "A data integration system that helps UNOPS send their data to USI system"
    PROJECT_VERSION :str = "0.1"
    
    BULKUPLOADFORMDATA_DESCRIPTION = "Create form data_in in the server using an xlsx, json , csv or xml format"
    BULKUPLOADFORMDATA_SUMMARY = "Create form data_in in the server using an xlsx, json , csv or xml format"
    
    QUESTIONS_SUMMARY="create a list of questions on our server and the output server"
    QUESTIONS_DESCRIPTION="create a list of questions on our server and the output server"
    
    QUESTION_UPDATE_SUMMARY = 'Update a question'
    QUESTION_UPDATE_DESCRIPTION = 'Update a question'
    
    FORMS_DESCRIPTION = "Create a list of forms on our server and the output server"
    FORMS_SUMMARY = "Create a list of forms on our server and the output server"
    
    FORM_QUESTIONS_DESCRIPTION = "Create a list of questions for a specific form using an excel "
    FORM_QUESTIONS_SUMMARY = "Create a list of questions for a specific form using an excel "
    
    CREATE_USER_DESCRIPTION = "create user in the db with hashed password"
    CREATE_USER_SUMMARY = "create user in the db with hashed password"
    
    GET_USER_DESCRIPTION = "get user in the db with hashed password"
    GET_USER_SUMMARY = "get user in the db with hashed password"
    
    GET_USERS_DESCRIPTION = "get users in the db with hashed password"
    GET_USERS_SUMMARY = "get users in the db with hashed password"
    
    
    

    

    
settings = Settings()
