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
package org.apache.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.airlift.testing.Closeables;
import org.apache.iceberg.catalog.Catalog;

import java.io.Closeable;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class DelegatingRESTSessionCatalog
        extends RESTSessionCatalog
{
    private RESTCatalogAdapter adapter;
    private Catalog delegate;

    DelegatingRESTSessionCatalog(RESTCatalogAdapter adapter, Catalog delegate)
    {
        super((properties) -> adapter);
        this.adapter = adapter;
        this.delegate = delegate;
    }

    @Override
    public void close()
            throws IOException
    {
        super.close();
        adapter.close();

        if (delegate instanceof Closeable) {
            Closeables.closeAll((Closeable) delegate);
        }
    }

    public TestingHttpServer testServer()
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpPort(0)
                .setMaxThreads(8)
                .setMinThreads(4)
                .setHttpAcceptorThreads(4)
                .setHttpAcceptQueueSize(10)
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);

        return new TestingHttpServer(httpServerInfo, nodeInfo, config, adapter.servlet(), ImmutableMap.of());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Catalog delegate;

        public Builder delegate(Catalog delegate)
        {
            this.delegate = delegate;
            return this;
        }

        public DelegatingRESTSessionCatalog build()
        {
            requireNonNull(delegate, "Delegate must be set");

            return new DelegatingRESTSessionCatalog(new RESTCatalogAdapter(delegate), delegate);
        }
    }
}
