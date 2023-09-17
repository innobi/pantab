# Not all types are writeable by pantab but should probably be readable
# This utility script will help generate those files which can be
# incorporate into testing

import tableauhyperapi as tab_api

if __name__ == "__main__":
    table = tab_api.TableDefinition(
        table_name=tab_api.TableName("public", "table"),
        columns=[
            tab_api.TableDefinition.Column(
                name="Non-Nullable String",
                type=tab_api.SqlType.text(),
                nullability=tab_api.NOT_NULLABLE,
            ),
            tab_api.TableDefinition.Column(
                name="Non-Nullable Float",
                type=tab_api.SqlType.double(),
                nullability=tab_api.NOT_NULLABLE,
            ),
        ],
    )

    with tab_api.HyperProcess(
        telemetry=tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hyper:
        with tab_api.Connection(
            endpoint=hyper.endpoint,
            database="non_pantab_writeable.hyper",
            create_mode=tab_api.CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            connection.catalog.create_table(table_definition=table)

            with tab_api.Inserter(connection, table) as inserter:
                inserter.add_rows([["row1", 1.0], ["row2", 2.0]])
                inserter.execute()
