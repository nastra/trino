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
package io.trino.plugin.iceberg.catalog.rest;

import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import org.apache.iceberg.rest.DelegatingRESTSessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.assertj.core.util.Files;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoRESTCatalog
        extends BaseTrinoCatalogTest
{
    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        File warehouseLocation = Files.newTemporaryFolder();
        warehouseLocation.deleteOnExit();

        String catalogName = "iceberg_rest";
        RESTSessionCatalog restSessionCatalog = DelegatingRESTSessionCatalog
                .builder()
                .delegate(RESTCatalogTestUtils.backendCatalog(warehouseLocation))
                .build();

        restSessionCatalog.initialize(catalogName, Collections.emptyMap());

        return new TrinoRESTCatalog(restSessionCatalog, new CatalogName(catalogName), "test");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("This connector does not support creating views");
    }

    @Override
    @Test
    public void testCreateNamespaceWithLocation()
    {
        assertThatThrownBy(super::testCreateNamespaceWithLocation)
                .hasMessageContaining("Database cannot be created with a location set");
    }

    @Override
    @Test
    public void testUseUniqueTableLocations()
    {
        assertThatThrownBy(super::testCreateNamespaceWithLocation)
                .hasMessageContaining("Database cannot be created with a location set");
    }
}
