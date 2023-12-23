# UnifillUpdateSparkPipeline

## Compile:
    sbt test package

## Run pipeline:
    /spark/bin/spark-submit \
        --conf spark.pipeline.fromInclusiveDate="2023-11-01 00:00:00" \
        --conf spark.pipeline.tillExclusiveDate="2023-11-01 00:01:00" \
        --properties-file src/main/resources/application.properties \
        --jars $(echo dependencies/*.jar | tr ' ' ',') \
        --master yarn \
        target/scala-2.12/unifillupdatesparkpipeline*.jar