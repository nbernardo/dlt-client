from flask.json.provider import DefaultJSONProvider
from datetime import time, date, datetime


def add_app_custom_handlers(app):
    """ 
        Set different Flask Application handlers 
            e.g. Time response handler
    """
    app.json_provider_class = CustomJSONProvider
    app.json = CustomJSONProvider(app)


class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, (time, date, datetime)):
            return obj.isoformat()
        return super().default(obj)
