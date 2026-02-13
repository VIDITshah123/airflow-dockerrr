from airflow.plugins_manager import AirflowPlugin


class MathFunctions:

    @staticmethod
    def add(a, b):
        return a + b

    @staticmethod
    def subtract(a, b):
        return a - b


class MathPlugin(AirflowPlugin):
    name = "math_plugin"
    operators = []
    hooks = []
    executors = []
    macros = [MathFunctions]
    admin_views = []
    flask_blueprints = []
    menu_links = []
