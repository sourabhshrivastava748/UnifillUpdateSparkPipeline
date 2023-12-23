# UnifillUpdateSparkPipeline

## Compile:
    sbt test package

## Run pipeline:
    /spark/bin/spark-submit \
       --conf spark.pipeline.fromInclusiveDate="2023-08-01 00:00:00" \
       --conf spark.pipeline.tillExclusiveDate="2023-09-01 00:00:00" \
       --master yarn \
       target/scala-2.12/unifillupdatesparkpipeline*.jar