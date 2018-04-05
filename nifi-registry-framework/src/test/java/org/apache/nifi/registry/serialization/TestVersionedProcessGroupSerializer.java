/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.registry.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TestVersionedProcessGroupSerializer {

    @Test
    public void testSerializeDeserializeFlowSnapshot() throws SerializationException {
        final Serializer<VersionedProcessGroup> serializer = new VersionedProcessGroupSerializer();

        final VersionedProcessGroup processGroup1 = new VersionedProcessGroup();
        processGroup1.setIdentifier("pg1");
        processGroup1.setName("My Process Group");

        final VersionedProcessor processor1 = new VersionedProcessor();
        processor1.setIdentifier("processor1");
        processor1.setName("My Processor 1");

        // make sure nested objects are serialized/deserialized
        processGroup1.getProcessors().add(processor1);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(processGroup1, out);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final VersionedProcessGroup deserializedProcessGroup1 = serializer.deserialize(in);

        Assert.assertEquals(processGroup1.getIdentifier(), deserializedProcessGroup1.getIdentifier());
        Assert.assertEquals(processGroup1.getName(), deserializedProcessGroup1.getName());

        Assert.assertEquals(1, deserializedProcessGroup1.getProcessors().size());

        final VersionedProcessor deserializedProcessor1 = deserializedProcessGroup1.getProcessors().iterator().next();
        Assert.assertEquals(processor1.getIdentifier(), deserializedProcessor1.getIdentifier());
        Assert.assertEquals(processor1.getName(), deserializedProcessor1.getName());

    }

    @Test
    public void test() throws IOException {
        parseHeader("/serialization/header.json");
    }

    @Test
    public void test2() throws IOException {
        parseHeader("/serialization/header-non-integer-version.json");
    }

    @Test
    public void test3() throws IOException {
        parseHeader("/serialization/header-no-version.json");
    }


    private void parseHeader(final String file) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectReader objectReader = objectMapper.reader();

        final int version;
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {

            final int maxHeaderBytes = 1024;
            final byte[] headerBytes = new byte[maxHeaderBytes];
            final int readHeaderBytes = is.read(headerBytes);

            // Seek '"header"'.
            final String headerKeyword = "\"header\"";
            final String headerStr = new String(headerBytes, StandardCharsets.UTF_8);
            final int headerIndex = headerStr.indexOf(headerKeyword);
            if (headerIndex < 0) {
                throw new SerializationException(String.format("Could not find %s in the first %d bytes",
                        headerKeyword, readHeaderBytes));
            }

            final int headerStart = headerStr.indexOf("{", headerIndex);
            if (headerStart < 0) {
                throw new SerializationException(String.format("Could not find '{' starting header object in the first %d bytes.", readHeaderBytes));
            }

            final int headerEnd = headerStr.indexOf("}", headerStart);
            if (headerEnd < 0) {
                throw new SerializationException(String.format("Could not find '}' ending header object in the first %d bytes.", readHeaderBytes));
            }

            final String headerObjectStr = headerStr.substring(headerStart, headerEnd + 1);
            System.out.printf("headerObjectStr=%s\n", headerObjectStr);

            final JsonNode header = objectReader.readTree(headerObjectStr);
            final JsonNode versionNode = header.get("formatVersion");
            if (versionNode == null) {
                throw new SerializationException("'formatVersion' was not found in the header.");
            }
            version = versionNode.asInt();

        }

        System.out.printf("version=%d\n", version);

        // Then parse the entire JSON.
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            final TypeReference<HashMap> typeRef
                    = new TypeReference<HashMap>() {};
            final VersionedProcessGroupContainer container = objectMapper.readValue(is, VersionedProcessGroupContainer.class);
            System.out.println(container);
        }
    }

    public static class VersionedProcessGroupContainer {
        private Map<String, Object> header;
        private VersionedProcessGroup content;

        public Map<String, Object> getHeader() {
            return header;
        }

        public void setHeader(Map<String, Object> header) {
            this.header = header;
        }

        public VersionedProcessGroup getContent() {
            return content;
        }

        public void setContent(VersionedProcessGroup content) {
            this.content = content;
        }
    }
}
