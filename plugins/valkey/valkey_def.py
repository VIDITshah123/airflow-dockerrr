from airflow.plugins_manager import AirflowPlugin
from valkey_utils import ValkeyConnector


class ValkeyPlugin(AirflowPlugin):

    name = "valkey_plugin"
    operators = []
    hooks = []
    macros = [ValkeyConnector]
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []