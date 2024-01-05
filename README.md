# UnifillUpdateSparkPipeline

## Compile:
    sbt test package

## Run pipeline:
    /spark/bin/spark-submit \
        --name "Unifill update - July" \
        --conf spark.pipeline.fromInclusiveDate="2023-07-01 00:00:00" \
        --conf spark.pipeline.tillExclusiveDate="2023-08-01 00:00:00" \
        --properties-file src/main/resources/application.properties \
        --jars $(echo dependencies/*.jar | tr ' ' ',') \
        --deploy-mode cluster \
        target/scala-2.12/unifillupdatesparkpipeline*.jar