import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Arquivo Trusted
ArquivoTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://720017990039speedtestglobalperformancetrusted/parquet/performance/type=fixed/year=2023/quarter=1/2023-01-01_performance_fixed_tiles.parquet/part-00000-05760d30-40c6-47b5-87b4-ca35d7123905-c000.snappy.parquet"
        ],
        "recurse": True,
    },
    transformation_ctx="ArquivoTrusted_node1",
)

# Script generated for node Arquivo Raw
ArquivoRaw_node1684368592671 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://720017990039speedtestglobalperformanceraw/raw/parquet/performance/type=fixed/year=2023/quarter=1/2023-01-01_performance_fixed_tiles.parquet"
        ],
        "recurse": True,
    },
    transformation_ctx="ArquivoRaw_node1684368592671",
)

# Script generated for node EDQ - Upload
EDQUpload_node1684366990471_ruleset = """
    # You may insert rules from the DQDL rule builder to the left.
    # Choose "+" to insert rule types and column schema into the code editor.
    # Rules are inserted at cursor position.
    # e.g. Completeness "colA" between 0.4 and 0.8 */
    Rules = [
        ColumnExists "avg_u_kbps",
        Completeness "avg_u_kbps" > 0.95,
        ColumnValues "avg_u_kbps" < 10000000,
        ColumnValues "avg_u_kbps" > 0
    ]
"""

EDQUpload_node1684366990471_DQ_Results = EvaluateDataQuality.apply(
    frame=ArquivoTrusted_node1,
    ruleset=EDQUpload_node1684366990471_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EDQUpload_node1684366990471",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
        "resultsS3Prefix": "s3://aws-glue-assets-720017990039-us-east-1/validation/",
    },
)
EDQUpload_node1684366990471 = EDQUpload_node1684366990471_DQ_Results

# Script generated for node EDQ - Download
EDQDownload_node1684366236907_ruleset = """
    # You may insert rules from the DQDL rule builder to the left.
    # Choose "+" to insert rule types and column schema into the code editor.
    # Rules are inserted at cursor position.
    # e.g. Completeness "colA" between 0.4 and 0.8 */
    Rules = [
        ColumnExists "avg_d_kbps",
        Completeness "avg_d_kbps" > 0.95,
        ColumnValues "avg_d_kbps" < 10000000,
        ColumnValues "avg_d_kbps" > 0
    ]
"""

EDQDownload_node1684366236907_DQ_Results = EvaluateDataQuality.apply(
    frame=ArquivoTrusted_node1,
    ruleset=EDQDownload_node1684366236907_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EDQDownload_node1684366236907",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
        "resultsS3Prefix": "s3://aws-glue-assets-720017990039-us-east-1/validation/",
    },
)
EDQDownload_node1684366236907 = EDQDownload_node1684366236907_DQ_Results

# Script generated for node EDQ - Chave
EDQChave_node1684364101907_ruleset = """
    # You may insert rules from the DQDL rule builder to the left.
    # Choose "+" to insert rule types and column schema into the code editor.
    # Rules are inserted at cursor position.
    # e.g. Completeness "colA" between 0.4 and 0.8 */
    Rules = [
        ColumnExists "quadkey",
        Completeness "quadkey" > 0.95,
        IsUnique "quadkey",
        IsPrimaryKey "quadkey"
    ]
"""

EDQChave_node1684364101907_DQ_Results = EvaluateDataQuality.apply(
    frame=ArquivoTrusted_node1,
    ruleset=EDQChave_node1684364101907_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EDQChave_node1684364101907",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
        "resultsS3Prefix": "s3://aws-glue-assets-720017990039-us-east-1/validation/",
    },
)
EDQChave_node1684364101907 = EDQChave_node1684364101907_DQ_Results

# Script generated for node IngestaoRaw
IngestaoRaw_node1684368634456_ruleset = """
    # You may insert rules from the DQDL rule builder to the left.
    # Choose "+" to insert rule types and column schema into the code editor.
    # Rules are inserted at cursor position.
    # e.g. Completeness "colA" between 0.4 and 0.8 */
    Rules = [
        RowCount > 1000
    ]
"""

IngestaoRaw_node1684368634456_DQ_Results = EvaluateDataQuality.apply(
    frame=ArquivoRaw_node1684368592671,
    ruleset=IngestaoRaw_node1684368634456_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "IngestaoRaw_node1684368634456",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
)
IngestaoRaw_node1684368634456 = IngestaoRaw_node1684368634456_DQ_Results

job.commit()
