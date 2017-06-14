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

package uk.ac.cam.cl.cadets.kafka.streams.reconciliation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

public final class TcpReconciliationRecord extends CadetsRecord {

    public final static String TCP_RECONCILIATION_EVENT =
        "recon:tcp";

    private String connectSockUuid;
    private String acceptSockUuid;
    private String method;
    private UUID authorityUuid;
    private float confidence;
    private long time;

    public TcpReconciliationRecord() {
    }

    public TcpReconciliationRecord(final String connectSockUuid,
        final String acceptSockUuid,
        final String method,
        final UUID authorityUuid,
        final float confidence) {
        super(TCP_RECONCILIATION_EVENT);
        this.connectSockUuid = connectSockUuid;
        this.acceptSockUuid = acceptSockUuid;
        this.method = method;
        this.authorityUuid = authorityUuid;
        this.confidence = confidence;
        this.time = Instant.now().toEpochMilli();
    }

    @JsonProperty("connect_sockuuid")
    public String getConnectSockUuid() {
        return connectSockUuid;
    }

    public void setConnectSockUuid(final String connectSockUuid) {
        this.connectSockUuid = connectSockUuid;
    }

    @JsonProperty("accept_sockuuid")
    public String getAcceptSockUuid() {
        return acceptSockUuid;
    }

    public void setAcceptSockUuid(final String acceptSockUuid) {
        this.acceptSockUuid = acceptSockUuid;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(final String method) {
        this.method= method;
    }

    @JsonProperty("authority_uuid")
    public UUID getAuthorityUuid() {
        return authorityUuid;
    }

    public void setAuthorityUuid(final UUID authorityUuid) {
        this.authorityUuid = authorityUuid;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setConfidence(final float confidence) {
        this.confidence = confidence;
    }

    public float getTime() {
        return time;
    }

    public void setTime(final long time) {
        this.time = time;
    }

    public JsonNode toJsonNode() {
        return new ObjectMapper().valueToTree(this);
    }
}
