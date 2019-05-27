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
package com.facebook.presto.hive.acid;

import java.util.Map;
import java.util.Objects;

class AcidNationRow
{
    int nationkey;
    String name;
    int regionkey;
    String comment;
    boolean isValid;

    public AcidNationRow(Map<String, Object> row)
    {
        this(
                (Integer) row.getOrDefault("n_nationkey", -1),
                (String) row.getOrDefault("n_name", "INVALID"),
                (Integer) row.getOrDefault("n_regionkey", -1),
                (String) row.getOrDefault("n_comment", "INVALID"),
                (Boolean) row.get("isValid"));
    }

    public AcidNationRow(int nationkey, String name, int regionkey, String comment, boolean isValid)
    {
        this.nationkey = nationkey;
        this.name = name;
        this.regionkey = regionkey;
        this.comment = comment;
        this.isValid = isValid;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, nationkey, regionkey, comment, isValid);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof AcidNationRow)) {
            return false;
        }

        AcidNationRow other = (AcidNationRow) obj;
        return (nationkey == other.nationkey
                && name.equals(other.name)
                && regionkey == other.regionkey
                && comment.equals(other.comment)
                && isValid == other.isValid);
    }
}
