from utils.logging.pipeline_logger_config import PipelineLogger as PL
from polars.dataframe import DataFrame

PipelineLogger = PL

def parse_aggregation(df: DataFrame, aggregation_list: list) -> DataFrame:

    aggregs = {}
    prev_aggregatio_field = None

    for aggreg in aggregation_list:
        if type(aggreg) != dict: continue

        if prev_aggregatio_field != None and prev_aggregatio_field != aggreg['field']:
            df_aggreg = df.group_by(prev_aggregatio_field).agg(
                aggregs[prev_aggregatio_field]
            )
            df = df.join(df_aggreg, on=prev_aggregatio_field, how="left")

        prev_aggregatio_field = aggreg['field']
        if not(prev_aggregatio_field in aggregs):
            aggregs[prev_aggregatio_field] = []
        aggregs[prev_aggregatio_field].append(aggreg['aggreg'])

    if prev_aggregatio_field != None:
        df_aggreg = df.group_by(prev_aggregatio_field).agg(
            aggregs[prev_aggregatio_field]
        )
        df = df.join(df_aggreg, on=prev_aggregatio_field, how="left")

    return df

"""
agg_df = (
    df.group_by("OrganizationLevel")
      .agg(...)
)

df = df.join(agg_df, on="OrganizationLevel", how="left")
"""