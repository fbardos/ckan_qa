RESULT_INSERT_COLLECTION = 'runs'

DEFAULT_SFTP_CONN_ID = 'sftp_bucket'
DEFAULT_MONGO_CONN_ID = 'mongodb_ckanqa'
DEFAULT_MATRIX_CONN_ID = 'matrix_fediverse'
DEFAULT_REDIS_CONN_ID = 'redis_cache'
DEFAULT_S3_CONN_ID = 'minio_storage'

MATRIX_ROOM_ID_ALL = 'ckanqa_matrix_room_all'
MATRIX_ROOM_ID_FAILURE = 'ckanqa_matrix_room_failure'

REDIS_DEFAULT_TTL = 60 * 5

ISO8601_BASIC_FORMAT = '%Y%m%dT%H%M%S.%fZ'  # Good for directory names (UTC only)

S3_BUCKET_NAME_DATA = 'ckanqa'
S3_BUCKET_NAME_META = 'great-expectations-meta'
