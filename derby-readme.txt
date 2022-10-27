Project Sources and Drivers
Data sources:
1) Apache derby (embedded) - metastore_db: General: Authentication: user and password, user APP, password <hidden>, URL: jdbc:derby:;databaseName=/Users/..../my-apache-spark-scala/metastore_db;create=true
2) Apache derby (remote) - metastore_db: General: Authentication: user and password, user APP, password <hidden>, URL: jdbc:derby://localhost:1527/metastore_db;create=true
3) Apache derby no create - metastore_db: General: Path: /Users/..../my-apache-spark-scala/metastore_db  Authentication: user and password, user APP, password <hidden>, URL: jdbc:derby:/Users/..../my-apache-spark-scala/metastore_db/metastore_db;create=true