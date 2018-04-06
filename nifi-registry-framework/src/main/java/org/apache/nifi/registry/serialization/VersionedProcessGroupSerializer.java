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

import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.serialization.jackson.JaxksonVersionedProcessGroupSerializer;
import org.apache.nifi.registry.serialization.jaxb.JAXBVersionedProcessGroupSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * A serializer for VersionedProcessGroup that maps a "version" of the data model to a serializer.
 * </p>
 *
 * <p>
 * When serializing, the default serializer is used.
 * The version will be written to a header at the beginning of the OutputStream then followed by the content.
 * </p>
 *
 * <p>
 * When deserializing, one of available serializers which can read the serialized content is used.
 * The header will first be read from the InputStream to determine the version by a serializer.
 * If the serializer successfully extracts the version, then it continues to deserialize the content.
 * Otherwise, the next available serializer will be asked to read the version.
 * </p>
 *
 * <p>
 * Each {@link VersionedSerializer} implementation is responsible to read all past data model versions.
 * When deserializeng old versions, the content should be migrated automatically to meet the current data model.
 * Current data model version is 1.
 * Data Model Version Histories:
 * <ul>
 *     <li>version 1: Initial version.</li>
 * </ul>
 * </p>
 */
@Service
public class VersionedProcessGroupSerializer implements Serializer<VersionedProcessGroup> {

    private static final Logger logger = LoggerFactory.getLogger(VersionedProcessGroupSerializer.class);

    static final Integer CURRENT_DATA_MODEL_VERSION = 1;

    private final List<VersionedSerializer<VersionedProcessGroup>> serializers;
    // TODO: Do we need to support configuring a specific serializer so that users can stay with the old format if needed?
    private final VersionedSerializer<VersionedProcessGroup> defaultSerializer;
    public static final int MAX_HEADER_BYTES = 1024;

    public VersionedProcessGroupSerializer() {

        final List<VersionedSerializer<VersionedProcessGroup>> tempSerializers = new ArrayList<>();
        tempSerializers.add(new JaxksonVersionedProcessGroupSerializer());
        tempSerializers.add(new JAXBVersionedProcessGroupSerializer());

        this.serializers = Collections.unmodifiableList(tempSerializers);
        this.defaultSerializer = tempSerializers.get(0);
    }

    @Override
    public void serialize(final VersionedProcessGroup versionedProcessGroup, final OutputStream out) throws SerializationException {

        defaultSerializer.serialize(CURRENT_DATA_MODEL_VERSION, versionedProcessGroup, out);
    }

    @Override
    public VersionedProcessGroup deserialize(final InputStream input) throws SerializationException {

        final InputStream markSupportedInput = input.markSupported() ? input : new BufferedInputStream(input);

        // Mark the beginning of the stream.
        markSupportedInput.mark(MAX_HEADER_BYTES);

        // Applying each serializer
        final List<SerializationException> errors = new ArrayList<>();
        for (VersionedSerializer<VersionedProcessGroup> serializer : serializers) {
            final int version;
            try {
                version = serializer.readDataModelVersion(input);
            } catch (SerializationException e) {
                errors.add(e);
                try {
                    input.reset();
                } catch (IOException resetException) {
                    // Should not happen.
                    throw new SerializationException("Unable to reset the input stream.", e);
                }
                continue;
            }

            return serializer.deserialize(version, markSupportedInput);
        }

        // TODO: log failed serializer and its reason.
        throw new SerializationException("Unable to find a process group serializer compatible with the input.");

    }

}
