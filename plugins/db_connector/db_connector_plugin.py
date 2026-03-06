from airflow.plugins_manager import AirflowPlugin
from logic import DBConnector

class DBConnectorPlugin(AirflowPlugin):
    name = "db_connector_plugin"
    operators = []
    hooks = []
    macros = [DBConnector]
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
