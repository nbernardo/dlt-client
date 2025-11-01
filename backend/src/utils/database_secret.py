from services.workspace.SecretManager import SecretManager

def postgres_secret(namespace, dbconfig):

    SecretManager.create_db_secret(namespace, dbconfig)