import dlt
from dlt.sources.sql_database import sql_table
from sqlalchemy import create_engine
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from cm_python_clients import LoadDataClient
import pandas as pd

@dlt.destination(batch_size=10000, name="clevermaps")
def clevermaps(items: TDataItems, table: TTableSchema) -> None:

    print(items)

    df = pd.DataFrame(items)
    df.to_csv("../data/cm_load_tmp.csv", index=False)

    access_token = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.h3QHjmL_mO0cWMJoXg550QYVnqAmfs0i0oOQnlKxcto8XbYbImtBiqbyOXzLkPGFD5CGq_HhSiBWEXKEmsUhpwpa3t4npnoK7SrYUIgoVtyidOYzxcWReyxIyzs0VX0pq_uEwxpq8WldK8hPJNgfBjIUj0p1VfT4HG_3TdLQyXKFl5UxHutYOzk-Z4KLDhl04XJxRb1KV5Dj_8KyRVIOGqvcgoSoKvy9_CM7DpflJIltNbP4ZBHt5s1Vc7GmENggODH_b0xMoe4KDiV4K9lpRSi4eLOf59RRAnyvkCBj409k43uT6Y7lNK9x6po-_LtSQe_32LDS_2NJdvvKxLMLNw.oK61s1oIDNn1dE1e.LYnjxyLtye6zF1D6LgfMK6O6ZC1U1MFfKHa1JR1DLCSnBfCojlAwGqXVVVHr-0dk4MZcYvnJADYTGZ0IUWNcWIDMxlSfJpijTZF0sSBXEmVdphHeB0Fm9tQwDFB5IvUivUojLgswLxD8OhQ1wUPYxAAJ_qzhFvPBsEV8j_ZRCiOvWeRL0jel2IvZmd2yWncD0IDemydnIqQ0SjX7OXOk-hdQSnYiEWZUSk6GlJTOLx7qQXAU30p73euffxaz9nYrXvNDFL9citVoSvpD5AVafWr-cvAotouEitIkdD7PYL5tIMadovYRNtzzdFBsppclNtFR-Uc4AJRf0GOpKFLA4KCiQxAcElyNGruncF_vyOQAigGpi7exADp8zWHYZB7o8PUP4AicCFhnJ8MAihwe7jSbWhjT1Wv8PICbkt3D-xl0dh-b6Tqg-rXV5_qywX1R8UxRDpS3IgwYlKHNSqlcoPT_1rjHeVon9xabpKUniQ1f3BeCwLo8jl7hf8S5U4Fr4JDvHvWYo3D6ha7G65rJIq-q7WjuWqnrKpKXVYFHXpZkSDEwJXFiw_e9tTvdevY0cYE8t4WE10U0t-9l53TT51TNppU-jS3f9CCkngKe4VKCDIkpZVI1aYz3Pc98AMCa-bFYjS41MY6tiW9O53orzhh97edxEiHgkrDtnyiBejjRX5kWl-xTnRCb9hua6kwiOPKqmJHH4F85uR2IOeEgJNeaYqzaoNi-zgp8CXfB9Iw5X2zv5f90sN-pYzEfb09ijX7tfcv8N2mtmo0o6Vgh3B4GbSVsNKGG8cleJQICX00dbZedxNXAPrBDmErUYeUgZeSf0w1I5I8NJcgCZjH-nDxJx_Z3Lhxsb1trTlk1moQsd8NOX8gqxgMViabJ7vmOUblKsOSy1uVu_5HMVacX-26SbV1vfnFEbsFI3tbiuLmgz32KzI5uYobDdhGR3HgDqh7r0a7zeuk_dMehvSCrRdw5aGPSO46e8HwvbSfKXSnX2js7p_mTWj3ENi2rzyhQg13ONLSz2yndW3fB5QNLpoQy1RoC_OopvcQgzpDTchVJWhq7_KkQKToHNmMKt2PQ9SDH2594y62xbFetOEFf8EA9Aw0YK4dthNxubnt4N__ksNBSUS3Yhwmt_VDylcFAqwrEcICMwAnf3UloDStmv_ZBj1N_Oq8ucjG3Odsbu0OfaXbUPA1h1Wtp5qslqGHu9vHzU6JRRasdUohlS4GoZeFez9H4sAa8MVA8bvo.RxJrSAPjbQ_baLLgiemBcQ"
    project_id = "c00hvfrpo6khijv1"
    csv_file = "../data/cm_load_tmp.csv"
    dataset = "stops_dwh"

    sdk = LoadDataClient(api_token=access_token)
    output_file = sdk.upload_data(project_id, csv_file, dataset)
    print(output_file)

engine = create_engine("duckdb:///../data/data.duckdb")
source = sql_table(engine, schema='app', table="stops_cm")

pipeline = dlt.pipeline(
    pipeline_name="duckdb_to_clevermaps",
    destination=clevermaps
)

info = pipeline.run(source)
print(info)


