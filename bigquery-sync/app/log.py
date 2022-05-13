import os, logging
from pyeslogging.handlers import PYESHandler
elk_enable = os.getenv('ELK_LOG_ENABLE',True)
elk_user = os.getenv('ELK_LOG_USERNAME', None)
elk_pass = os.getenv('ELK_LOG_PASSWORD', None)
elk_https = os.getenv('ELK_LOG_HTTPS', False)
es_index_name = "bi-sync"

if elk_user is not None and elk_pass is not None:
    handler = PYESHandler(hosts=[{'host': os.getenv('ELK_LOG_URI', 'elk-es-http.elk.svc'), 'port': 9200}],
                          auth_type=PYESHandler.AuthType.BASIC_AUTH,
                          auth_details={"username": os.getenv('ELK_LOG_USERNAME'),
                                        "password": os.getenv('ELK_LOG_PASSWORD')},
                          es_index_name=es_index_name,
                          # index_name_frequency=PYESHandler.ElasticECSHandler.IndexNameFrequency.DAILY,
                          use_ssl=elk_https,
                          verify_ssl=elk_https)
else:
    handler = PYESHandler(hosts=[{'host': os.getenv('ELK_LOG_URI', 'localhost'), 'port': 9200}],
                          es_index_name=es_index_name,
                          # index_name_frequency=PYESHandler.ElasticECSHandler.IndexNameFrequency.DAILY,
                          use_ssl=elk_https,
                          verify_ssl=elk_https)

logger = logging.getLogger("bi-sync")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

if elk_enable is True:
    logger.info("Enable elasticsearch handler")
    logger.addHandler(handler)

for _ in ("boto", "elasticsearch", "urllib3"):
    logging.getLogger(_).setLevel(logging.CRITICAL)