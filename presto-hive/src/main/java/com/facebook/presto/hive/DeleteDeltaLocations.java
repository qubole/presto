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
package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/*
 * Stores information about DELETE_DELTAs for a Partition
 */
public class DeleteDeltaLocations
{
    // A map of partition location to DeleteDeltaInfo objects, if partition has moved and we end up with multiple locations for DELETE_DELTAS
    // Most of the DeleteDeltaInfo objects share same location, this scheme reduces the size sent over wire
    private final Map<String, List<DeleteDeltaInfo>> deleteDeltas;

    public DeleteDeltaLocations()
    {
        this(new HashMap());
    }

    @JsonCreator
    public DeleteDeltaLocations(@JsonProperty("deleteDeltas") Map<String, List<DeleteDeltaInfo>> deleteDeltas)
    {
        this.deleteDeltas = deleteDeltas;
    }

    @JsonProperty
    public Map<String, List<DeleteDeltaInfo>> getDeleteDeltas()
    {
        return deleteDeltas;
    }

    public void addDeleteDelta(Path deletaDeltaPath, long minWriteId, long maxWriteId, int statementId)
    {
        String partitionLocation = deletaDeltaPath.getParent().toString();
        List<DeleteDeltaInfo> deleteDeltas = this.deleteDeltas.getOrDefault(partitionLocation, new ArrayList());
        deleteDeltas.add(new DeleteDeltaInfo(minWriteId, maxWriteId, statementId));
        this.deleteDeltas.put(partitionLocation, deleteDeltas);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(deleteDeltas);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (!(o instanceof DeleteDeltaLocations)) {
            return false;
        }

        DeleteDeltaLocations other = (DeleteDeltaLocations) o;

        if (Sets.difference(deleteDeltas.keySet(), other.deleteDeltas.keySet()).size() != 0) {
            return false;
        }

        for (String key : deleteDeltas.keySet()) {
            if (!deleteDeltas.get(key).equals(other.deleteDeltas.get(key))) {
                return false;
            }
        }

        return true;
    }

    public static class DeleteDeltaInfo
    {
        private final long minWriteId;
        private final long maxWriteId;
        private final int statementId;

        @JsonCreator
        public DeleteDeltaInfo(
                @JsonProperty("minWriteId") long minWriteId,
                @JsonProperty("maxWriteId") long maxWriteId,
                @JsonProperty("statementId") int statementId)
        {
            this.minWriteId = minWriteId;
            this.maxWriteId = maxWriteId;
            this.statementId = statementId;
        }

        @JsonProperty
        public long getMinWriteId()
        {
            return minWriteId;
        }

        @JsonProperty
        public long getMaxWriteId()
        {
            return maxWriteId;
        }

        @JsonProperty
        public int getStatementId()
        {
            return statementId;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .addValue(minWriteId)
                    .addValue(maxWriteId)
                    .addValue(statementId)
                    .toString();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(minWriteId, maxWriteId, statementId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (!(o instanceof DeleteDeltaInfo)) {
                return false;
            }

            DeleteDeltaInfo other = (DeleteDeltaInfo) o;
            if (minWriteId == other.minWriteId && maxWriteId == other.maxWriteId && statementId == other.statementId) {
                return true;
            }

            return false;
        }
    }
}
