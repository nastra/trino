/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.nessie;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;

public class TestIcebergParquetNessieConnectorTest
        extends BaseIcebergNessieConnectorTest
{
    public TestIcebergParquetNessieConnectorTest()
    {
        super(PARQUET);
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkParquetFileSorting(path, sortColumnName);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return true;
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary") ||
                typeName.equalsIgnoreCase("time(6)") ||
                typeName.equalsIgnoreCase("timestamp(6) with time zone"));
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        // TODO https://github.com/trinodb/trino/issues/15822 The connector returns incorrect NULL when a field in row type doesn't exist in Parquet files
        if ("row(x integer) -> row(y integer)".equals("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType()))) {
            return Optional.of(setup.withNewValueLiteral("NULL"));
        }
        return super.filterSetColumnTypesDataProvider(setup);
    }
}
