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
package io.trino.plugin.databricks;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDatabricksConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DatabricksConfig.class)
                .setDisableAutomaticFetchSize(false)
                .setArrayMapping(DatabricksConfig.ArrayMapping.DISABLED)
                .setIncludeSystemTables(false)
                .setEnableStringPushdownWithCollate(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("postgresql.disable-automatic-fetch-size", "true")
                .put("postgresql.array-mapping", "AS_ARRAY")
                .put("postgresql.include-system-tables", "true")
                .put("postgresql.experimental.enable-string-pushdown-with-collate", "true")
                .buildOrThrow();

        DatabricksConfig expected = new DatabricksConfig()
                .setDisableAutomaticFetchSize(true)
                .setArrayMapping(DatabricksConfig.ArrayMapping.AS_ARRAY)
                .setIncludeSystemTables(true)
                .setEnableStringPushdownWithCollate(true);

        assertFullMapping(properties, expected);
    }
}
