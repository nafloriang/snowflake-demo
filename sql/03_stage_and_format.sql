CREATE OR REPLACE FILE FORMAT json_ff
TYPE = JSON;

CREATE OR REPLACE STAGE landing_stage
URL = 'gcs://autonomous-vault-landing/landing/'
STORAGE_INTEGRATION = gcs_int
FILE_FORMAT = json_ff;
