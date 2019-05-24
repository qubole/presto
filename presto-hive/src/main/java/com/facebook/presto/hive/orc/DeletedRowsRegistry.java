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
package com.facebook.presto.hive.orc;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class DeletedRowsRegistry
{
    public Block createIsValidBlock(Block orignalTxn, Block bucket, Block rowId)
    {
        BlockBuilder isValidBlock = BOOLEAN.createFixedSizeBlockBuilder(rowId.getPositionCount());
        for (int pos = 0; pos < rowId.getPositionCount(); pos++) {
            // TODO stagra: Add logic to check {originalTxn, bucket, rowId} present in registry
            BOOLEAN.writeBoolean(isValidBlock, true);
        }
        return isValidBlock.build();
    }
}
