/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.utils;

public class BytesUtil {

    /**
     * 将整型转换为字节数据
     * 整型 = 32位 = 4 * 8
     * 每次通过移位操作将某8位数据移动到低8位，然后通过与操作，清除高24位的数据，然后收集该8位的值。
     */
    public static byte[] intToBytes(int value) {
        byte[] src = new byte[4];
        src[3] = (byte) ((value >> 24) & 0xFF);
        src[2] = (byte) ((value >> 16) & 0xFF);
        src[1] = (byte) ((value >> 8) & 0xFF);
        src[0] = (byte) (value & 0xFF);
        return src;
    }

    public static int bytesToInt(byte[] src, int offset) {
        // short、byte、char 移位操作前 会先将其转为int 再处理
        // long 左移后的值是long
        int value;
        value = (int) ((src[offset] & 0xFF)
            | ((src[offset + 1] & 0xFF) << 8)
            | ((src[offset + 2] & 0xFF) << 16)
            | ((src[offset + 3] & 0xFF) << 24));
        return value;
    }

    public static void main(String[] args) {
        int b = 3;
        System.out.println(Long.class.isInstance(b << 35));
    }
}
