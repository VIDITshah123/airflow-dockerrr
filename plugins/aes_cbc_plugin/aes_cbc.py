from airflow.plugins_manager import AirflowPlugin
from cbc_logic import AESCrypto


class AESCryptoPlugin(AirflowPlugin):

    name = "aes_crypto_plugin"
    operators = []
    hooks = []
    macros = [AESCrypto]
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []