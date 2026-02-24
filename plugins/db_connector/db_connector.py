from airflow.plugins_manager import AirflowPlugin


class DBConnector(AirflowPlugin):
    name = "postgres_plugin"
    operators = []
    hooks = []
    macros = [DBConnector]
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []