from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize the connection to the cluster
glueContext = GlueContext(SparkContext.getOrCreate())

# S3 location for output
output_dir = "s3://<BUCKET_NAME>/parking_tickets/output/"

# Read data into a DynamicFrame using the Data Catalog metadata
df = glueContext.create_dynamic_frame.from_catalog(
    database="infractions",
    table_name="parking_ticketsinput"
)

# We can use the lower-level DataFrame to rename a column:
# Convert to data frame and rename a column
df_new = (
    df.toDF()
    .select(
        "tag_number_masked", 
        "date_of_infraction", 
        "infraction_code", 
        "infraction_description", 
        "set_fine_amount", 
        "time_of_infraction"
    )
)


# Convert back to a dynamic frame
dyF_clean = DynamicFrame.fromDF(df_new, glueContext, "tickets_clean_dyf")


# Write it out in Parquet
sink = glueContext.getSink(connection_type="s3", path=output_dir,
    enableUpdateCatalog=True, updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[])
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase="infractions", catalogTableName="new_ticket_clean")
sink.writeFrame(dyF_clean)



### https://docs.aws.amazon.com/glue/latest/dg/update-from-job.html
### https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame-writer.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-writer-from_catalog
