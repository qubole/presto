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
package com.facebook.presto.orc;

public class ACIDConstants
{
    public static final int ACID_META_COLS_COUNT = 5;

    public static final int ACID_OPERATION_INDEX = 0;
    public static final int ACID_ORIGINAL_TXN_INDEX = 1;
    public static final int ACID_BUCKET_INDEX = 2;
    public static final int ACID_ROWID_INDEX = 3;
    public static final int ACID_CURRENT_TXN_INDEX = 4;
    public static final int ACID_ROW_STRUCT_INDEX = 6;

    public static final String[] ACID_META_COLUMNS = {"operation", "originalTransaction", "bucket", "rowId", "currentTransaction"};

    private ACIDConstants()
    {
    }
}
