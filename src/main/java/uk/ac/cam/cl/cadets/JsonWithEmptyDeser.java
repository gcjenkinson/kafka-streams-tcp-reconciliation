/*-
* Copyright (c) 2017 (Graeme Jenkinson)
* All rights reserved.
*
* This software was developed by BAE Systems, the University of Cambridge
* Computer Laboratory, and Memorial University under DARPA/AFRL contract
* FA8650-15-C-7558 ("CADETS"), as part of the DARPA Transparent Computing
* (TC) research program.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions
* are met:
* 1. Redistributions of source code must retain the above copyright
* notice, this list of conditions and the following disclaimer.
* 2. Redistributions in binary form must reproduce the above copyright
* notice, this list of conditions and the following disclaimer in the
* documentation and/or other materials provided with the distribution.
*
* THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
* ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
* FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
* DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
* OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
* LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
* OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
* SUCH DAMAGE.
*
*/

package uk.ac.cam.cl.cadets;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JsonWithEmptyDeser implements Deserializer<JsonNode> {

    private static final Logger LOGGER =
         LoggerFactory.getLogger(JsonWithEmptyDeser.class);

    Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

    @Override
    public void configure(final Map<String, ?> map, final boolean b) {
        jsonDeserializer.configure(map, b);
    }

    @Override
    public JsonNode deserialize(final String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
	    return null;
        } else {
	    try {
                try {
		    return jsonDeserializer.deserialize(s,
                        new String(bytes, "ISO-8859-1")
                            .replaceAll("^\\s*,","").getBytes());
	        } catch (SerializationException e) {
		    LOGGER.warn("Exception during deserialization of string:\n"
                        + (new String(bytes, "UTF-8")), e);
		    return null;
                }
	    } catch(UnsupportedEncodingException uee) {
		LOGGER.warn("Exception during deserialization of string");
                return null;
            }
        }
    }

    @Override
    public void close() {
        jsonDeserializer.close();
    }
}
