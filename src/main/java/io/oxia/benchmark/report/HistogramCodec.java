/*
 * Copyright © 2025 The Oxia Authors
 *
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
package io.oxia.benchmark.report;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.DataFormatException;
import org.HdrHistogram.DoubleHistogram;

/** Base64 + compressed-HdrHistogram (de)serialization, used to ship per-workload histograms. */
public final class HistogramCodec {

    private HistogramCodec() {}

    public static String encode(DoubleHistogram h) {
        ByteBuffer buf = ByteBuffer.allocate(h.getNeededByteBufferCapacity());
        int len = h.encodeIntoCompressedByteBuffer(buf);
        return Base64.getEncoder().encodeToString(Arrays.copyOf(buf.array(), len));
    }

    public static DoubleHistogram decode(String b64) {
        try {
            return DoubleHistogram.decodeFromCompressedByteBuffer(
                    ByteBuffer.wrap(Base64.getDecoder().decode(b64)), 0);
        } catch (DataFormatException e) {
            throw new IllegalArgumentException("corrupt histogram blob", e);
        }
    }
}
