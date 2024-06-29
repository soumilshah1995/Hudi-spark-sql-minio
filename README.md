# Hudi-spark-sql-minio



docker-compose.yaml
```
version: "3"

services:


  metastore_db:
    image: postgres:11
    hostname: metastore_db
    ports:
      - 5432:5432
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
      POSTGRES_DB: metastore

  hive-metastore:
    hostname: hive-metastore
    image: 'starburstdata/hive:3.1.2-e.18'
    ports:
      - '9083:9083' # Metastore Thrift
    environment:
      HIVE_METASTORE_DRIVER: org.postgresql.Driver
      HIVE_METASTORE_JDBC_URL: jdbc:postgresql://metastore_db:5432/metastore
      HIVE_METASTORE_USER: hive
      HIVE_METASTORE_PASSWORD: hive
      HIVE_METASTORE_WAREHOUSE_DIR: s3://datalake/
      S3_ENDPOINT: http://minio:9000
      S3_ACCESS_KEY: admin
      S3_SECRET_KEY: password
      S3_PATH_STYLE_ACCESS: "true"
      REGION: ""
      GOOGLE_CLOUD_KEY_FILE_PATH: ""
      AZURE_ADL_CLIENT_ID: ""
      AZURE_ADL_CREDENTIAL: ""
      AZURE_ADL_REFRESH_URL: ""
      AZURE_ABFS_STORAGE_ACCOUNT: ""
      AZURE_ABFS_ACCESS_KEY: ""
      AZURE_WASB_STORAGE_ACCOUNT: ""
      AZURE_ABFS_OAUTH: ""
      AZURE_ABFS_OAUTH_TOKEN_PROVIDER: ""
      AZURE_ABFS_OAUTH_CLIENT_ID: ""
      AZURE_ABFS_OAUTH_SECRET: ""
      AZURE_ABFS_OAUTH_ENDPOINT: ""
      AZURE_WASB_ACCESS_KEY: ""
      HIVE_METASTORE_USERS_IN_ADMIN_ROLE: "admin"
    depends_on:
      - metastore_db
    healthcheck:
      test: bash -c "exec 6<> /dev/tcp/localhost/9083"

  minio:
    image: minio/minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
    networks:
      default:
        aliases:
          - warehouse.minio
    ports:
      - 9001:9001
      - 9000:9000
    command: ["server", "/data", "--console-address", ":9001"]

  mc:
    depends_on:
      - minio
    image: minio/mc
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc policy set public minio/warehouse;
      tail -f /dev/null
      "



volumes:
  hive-metastore-postgresql:

networks:
  default:
    name: hudi
```

# sql 

```
spark-sql \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2' \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
    --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
    --conf "spark.sql.catalogImplementation=hive" \
    --conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083" \
    --conf "spark.hadoop.fs.s3a.access.key=admin" \
    --conf "spark.hadoop.fs.s3a.secret.key=password" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "fs.s3a.signing-algorithm=S3SignerType"

```

```
DROP TABLE IF EXISTS default.hudi_table;


SET hoodie.enable.data.skipping=true;
SET hoodie.metadata.column.stats.enable=true;
SET hoodie.metadata.enable=true;
SET hoodie.metadata.record.index.enable=true;

CREATE TABLE hudi_table (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING hudi
OPTIONS (
    primaryKey = 'uuid',
    preCombineField = 'ts',
    path 's3a://warehouse/default/table_name=hudi_table'
)
PARTITIONED BY (city);



INSERT INTO hudi_table
VALUES
(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
(1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
(1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
(1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
(1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'    ),
(1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'    ),
(1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'      ),
(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');


 SELECT ts, fare, rider, driver, city FROM  hudi_table WHERE fare > 20.0;

UPDATE hudi_table SET fare = 25.0 WHERE rider = 'rider-D';
SELECT * FROM hudi_table WHERE rider = 'rider-D';

DELETE FROM hudi_table WHERE uuid = '3f3d9565-7261-40e6-9b39-b8aa784f95e2';


SELECT * FROM hudi_table_changes('default.hudi_table', 'latest_state', 'earliest');

SELECT * FROM hudi_table
WHERE uuid = 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa';

SELECT * FROM hudi_table
WHERE fare > 100000.0 ;

call show_commits(table => 'hudi_table', limit => 10);

call show_commits_metadata(table => 'hudi_table');
```
