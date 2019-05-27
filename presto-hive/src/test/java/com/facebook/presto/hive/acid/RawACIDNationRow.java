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

class RawACIDNationRow
{
    // prefilled values are the fixed ones in nationFile25kRowsSortedOnNationKey.orc
    int operation;
    long originalTransaction;
    int bucket;
    long rowId;
    long currentTransaction;
    int nationkey;
    String name;
    int regionkey;
    String comment;

    public RawACIDNationRow(Map<String, Object> row)
    {
        this(
                (Integer) row.getOrDefault("operation", -1),
                (Long) row.getOrDefault("originalTransaction", -1),
                (Integer) row.getOrDefault("bucket", -1),
                (Long) row.getOrDefault("rowId", -1),
                (Long) row.getOrDefault("currentTransaction", -1),
                (Integer) row.getOrDefault("n_nationkey", -1),
                (String) row.getOrDefault("n_name", "INVALID"),
                (Integer) row.getOrDefault("n_regionkey", -1),
                (String) row.getOrDefault("n_comment", "INVALID"));
    }

    public RawACIDNationRow(int operation, long originalTransaction, int bucket, long rowId, long currentTransaction, int nationkey, String name, int regionkey, String comment)
    {
        this.operation = operation;
        this.originalTransaction = originalTransaction;
        this.bucket = bucket;
        this.rowId = rowId;
        this.currentTransaction = currentTransaction;
        this.nationkey = nationkey;
        this.name = name;
        this.regionkey = regionkey;
        this.comment = comment;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operation, originalTransaction, bucket, rowId, currentTransaction, name, nationkey, regionkey, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof RawACIDNationRow)) {
            return false;
        }

        RawACIDNationRow other = (RawACIDNationRow) obj;
        return (operation == other.operation
                && originalTransaction == other.originalTransaction
                && bucket == other.bucket
                && rowId == other.rowId
                && currentTransaction == other.currentTransaction
                && nationkey == other.nationkey
                && name.equals(other.name)
                && regionkey == other.regionkey
                && comment.equals(other.comment));
    }
}
