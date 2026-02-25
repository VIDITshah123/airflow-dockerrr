from airflow.plugins_manager import AirflowPlugin


class SMTPConnector(AirflowPlugin):
    name = "email_plugin"
    operators = []
    hooks = []
    macros = [SMTPConnector]
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []