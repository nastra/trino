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

import io.trino.filesystem.Location;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;

public class TestIcebergOrcNessieConnectorTest
        extends BaseIcebergNessieConnectorTest
{
    public TestIcebergOrcNessieConnectorTest()
    {
        super(ORC);
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkOrcFileSorting(fileSystemFactory, Location.of(path), sortColumnName);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary")) &&
                !(typeName.equalsIgnoreCase("uuid"));
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary");
    }
}
