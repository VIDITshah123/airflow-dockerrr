from airflow.plugins_manager import AirflowPlugin
from gcm_logic import AESGCMCrypto


class AESGCMCryptoPlugin(AirflowPlugin):

    name = "aes_crypto_plugin_gcm"
    operators = []
    hooks = []
    macros = [AESGCMCrypto]
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []