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
package com.facebook.presto.hive.parquet.dictionary;

import com.facebook.presto.hive.parquet.ParquetDictionaryPage;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ParquetFloatDictionary
        extends ParquetDictionary
{
    private final float[] content;

    public ParquetFloatDictionary(ParquetDictionaryPage dictionaryPage)
            throws IOException
    {
        super(dictionaryPage.getEncoding());
        content = new float[dictionaryPage.getDictionarySize()];
        FloatPlainValuesReader floatReader = new FloatPlainValuesReader();
        ByteBuffer byteBuffer = ByteBuffer.wrap(dictionaryPage.getSlice().getBytes());
        ByteBufferInputStream inputStream = ByteBufferInputStream.wrap(ImmutableList.of(byteBuffer));
        floatReader.initFromPage(dictionaryPage.getDictionarySize(), inputStream);
        for (int i = 0; i < content.length; i++) {
            content[i] = floatReader.readFloat();
        }
    }

    @Override
    public float decodeToFloat(int id)
    {
        return content[id];
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("content", content)
                .toString();
    }
}
