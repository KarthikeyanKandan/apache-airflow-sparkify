from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key')
    copy_sql = """
        COPY {}
        FROM {}
        ACCESS_KEY_ID {}
        SECRET_ACCESS_KEY {}
        REGION {}
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS
        BLANKASNULL
        EMPTYASNULL
        {} 'auto'
        {}
     """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 region = "",
                 file_format = "JSON",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        """
            Copy data from S3 buckets to staging tables in redshift cluster.
                - redshift_conn_id: redshift cluster connection id
                - aws_credentials_id: AWS connection credentials
                - table: redshift cluster table name
                - s3_bucket: S3 bucket name of source data
                - s3_key: S3 key files of source data
                - file_format: source file format - options JSON, CSV
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('Delete existing data from table')
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Copy data from S3 to Redshift')
        s3_path = "s3.//{}".format(self.s3_bucket)
        #Backfill to certain date
        if self.execution_date:
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            s3_path = "/".join(s3_path, str(year), str(month), str(day))
        s3_path = s3_path + "/" + self.s3_key

        additional = ""
        if file_format=="CSV":
            additional=" DELIMITER ',' IGNOREHEADER 1 "
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            additional
        )

        redshift.run(formatted_sql)
        self.log.info('Success: Copied data from {self.s3_bucket} S3 bucket to {self.table} staging table in Redshift')





