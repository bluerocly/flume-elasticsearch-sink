/*
 * Copyright 2017 Cognitree Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.cognitree.flume.sink.elasticsearch;

import static com.cognitree.flume.sink.elasticsearch.Constants.DEFAULT_DELAY_DAYS;
import static com.cognitree.flume.sink.elasticsearch.Constants.DEFAULT_ES_TYPE;
import static com.cognitree.flume.sink.elasticsearch.Constants.DELAY_DAYS;
import static com.cognitree.flume.sink.elasticsearch.Constants.ES_INDEX;
import static com.cognitree.flume.sink.elasticsearch.Constants.ES_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by prashant
 */
public class TestEventTimeIndexBuilder {

    private EventTimeBasedIndexBuilder eventTimeBasedIndexBuilder;

    private String index = "es-index";

    private String type = "es-type";

    private int delayDays = 6;

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"); 
    
    @Before
    public void init() throws Exception {
    	eventTimeBasedIndexBuilder = new EventTimeBasedIndexBuilder();
    }

    /**
     * tests header based index, type and id
     */
    @Test
    public void testEventTimeIndex() {
    	Context context = new Context();
        context.put(ES_INDEX, index);
        context.put(ES_TYPE, type);
        context.put(DELAY_DAYS, ""+delayDays);
        eventTimeBasedIndexBuilder.configure(context);
         
        Event event = new SimpleEvent();
        DateTime dateTime = new DateTime();
        String eventJson = "{\"eventTime\":\"" + dateTime.minusDays(delayDays+1).toString(formatter) + "\",\"province\":\"广东\",\"city\":\"广州\",\"domain\":\"PS域\",\"neName\":\"GZMME1301BEr\",\"neType\":\"MME\",\"vendor\":\"爱立信\",\"body\":\"======== SESSION EVENT (W): MS INITIATED ACTIVATE REQUEST ========= Time           : 2017-10-10 10:49:22  Node           : e_Erlang__Global_pm3_9_2_1@eqm03s09p2 IMSI           : 460025120132401 MSISDN         : 8618312019433 SM Cause       : Network Failure (#38) Details        : Unknown RNC error: 239 PDP Addr       : 10.4.106.134 APN Used       : cmnet.mnc002.mcc460.gprs RNC Id         : 669\",\"logType\":\"session_event_log\",\"fileName\":\"188.1.92.106_session_event_log.4.txt\",\"logModule\":\"\",\"timenano\":10608626861853233,\"logLevel\":\"\",\"neNumber\":\"SGSN MKVIII\",\"netWork\":\"WCDMA\",\"node\":\"e_Erlang__Global_pm3_9_2_1@eqm03s09p2\",\"imsi\":\"460025120132401\",\"msisdn\":\"8618312019433\",\"sm_cause\":\"Network Failure (#38)\",\"details\":\"Unknown\",\"message\":\"\",\"pdp_addr\":\"10.4.106.134\",\"apn_used\":\"cmnet.mnc002.mcc460.gprs\",\"rnc_id\":\"669\",\"pdp_type\":\"\",\"eNodeB_id_type\":\"\",\"eNodeB_id\":\"\",\"eNodeB_plmn_id\":\"\",\"apn_req\":\"\",\"apn_sub\":\"\",\"pdn_addr\":\"\",\"sgw_addr\":\"\",\"k1\":\"Network Failure\"}";
        event.setBody(eventJson.getBytes());
        assertNull(eventTimeBasedIndexBuilder.getIndex(event));
        
        eventJson = "{\"eventTime\":\"" + dateTime.plusMinutes(5).toString(formatter) + "\",\"province\":\"广东\",\"city\":\"广州\",\"domain\":\"PS域\",\"neName\":\"GZMME1301BEr\",\"neType\":\"MME\",\"vendor\":\"爱立信\",\"body\":\"======== SESSION EVENT (W): MS INITIATED ACTIVATE REQUEST ========= Time           : 2017-10-10 10:49:22  Node           : e_Erlang__Global_pm3_9_2_1@eqm03s09p2 IMSI           : 460025120132401 MSISDN         : 8618312019433 SM Cause       : Network Failure (#38) Details        : Unknown RNC error: 239 PDP Addr       : 10.4.106.134 APN Used       : cmnet.mnc002.mcc460.gprs RNC Id         : 669\",\"logType\":\"session_event_log\",\"fileName\":\"188.1.92.106_session_event_log.4.txt\",\"logModule\":\"\",\"timenano\":10608626861853233,\"logLevel\":\"\",\"neNumber\":\"SGSN MKVIII\",\"netWork\":\"WCDMA\",\"node\":\"e_Erlang__Global_pm3_9_2_1@eqm03s09p2\",\"imsi\":\"460025120132401\",\"msisdn\":\"8618312019433\",\"sm_cause\":\"Network Failure (#38)\",\"details\":\"Unknown\",\"message\":\"\",\"pdp_addr\":\"10.4.106.134\",\"apn_used\":\"cmnet.mnc002.mcc460.gprs\",\"rnc_id\":\"669\",\"pdp_type\":\"\",\"eNodeB_id_type\":\"\",\"eNodeB_id\":\"\",\"eNodeB_plmn_id\":\"\",\"apn_req\":\"\",\"apn_sub\":\"\",\"pdn_addr\":\"\",\"sgw_addr\":\"\",\"k1\":\"Network Failure\"}";
        event.setBody(eventJson.getBytes());
        assertNull(eventTimeBasedIndexBuilder.getIndex(event));
        
        eventJson = "{\"eventTime\":\"" + dateTime.minusMinutes(5).toString(formatter) + "\",\"province\":\"广东\",\"city\":\"广州\",\"domain\":\"PS域\",\"neName\":\"GZMME1301BEr\",\"neType\":\"MME\",\"vendor\":\"爱立信\",\"body\":\"======== SESSION EVENT (W): MS INITIATED ACTIVATE REQUEST ========= Time           : 2017-10-10 10:49:22  Node           : e_Erlang__Global_pm3_9_2_1@eqm03s09p2 IMSI           : 460025120132401 MSISDN         : 8618312019433 SM Cause       : Network Failure (#38) Details        : Unknown RNC error: 239 PDP Addr       : 10.4.106.134 APN Used       : cmnet.mnc002.mcc460.gprs RNC Id         : 669\",\"logType\":\"session_event_log\",\"fileName\":\"188.1.92.106_session_event_log.4.txt\",\"logModule\":\"\",\"timenano\":10608626861853233,\"logLevel\":\"\",\"neNumber\":\"SGSN MKVIII\",\"netWork\":\"WCDMA\",\"node\":\"e_Erlang__Global_pm3_9_2_1@eqm03s09p2\",\"imsi\":\"460025120132401\",\"msisdn\":\"8618312019433\",\"sm_cause\":\"Network Failure (#38)\",\"details\":\"Unknown\",\"message\":\"\",\"pdp_addr\":\"10.4.106.134\",\"apn_used\":\"cmnet.mnc002.mcc460.gprs\",\"rnc_id\":\"669\",\"pdp_type\":\"\",\"eNodeB_id_type\":\"\",\"eNodeB_id\":\"\",\"eNodeB_plmn_id\":\"\",\"apn_req\":\"\",\"apn_sub\":\"\",\"pdn_addr\":\"\",\"sgw_addr\":\"\",\"k1\":\"Network Failure\"}";
        event.setBody(eventJson.getBytes());
        String stdIndex = index + dateTime.toString("yyyy-MM-dd");
        assertEquals(stdIndex, eventTimeBasedIndexBuilder.getIndex(event));
        
        assertEquals(type, eventTimeBasedIndexBuilder.getType(event));
        assertEquals(delayDays, eventTimeBasedIndexBuilder.getDelayDays(event));
    }

    /**
     * tests configuration based index and type
     */
    @Test
    public void testConfigurationIndex() {
        Event event = new SimpleEvent();
        DateTime dateTime = new DateTime();
        String eventJson = "{\"eventTime\":\"" + dateTime.minusMinutes(5).toString(formatter) + "\",\"province\":\"广东\",\"city\":\"广州\",\"domain\":\"PS域\",\"neName\":\"GZMME1301BEr\",\"neType\":\"MME\",\"vendor\":\"爱立信\",\"body\":\"======== SESSION EVENT (W): MS INITIATED ACTIVATE REQUEST ========= Time           : 2017-10-10 10:49:22  Node           : e_Erlang__Global_pm3_9_2_1@eqm03s09p2 IMSI           : 460025120132401 MSISDN         : 8618312019433 SM Cause       : Network Failure (#38) Details        : Unknown RNC error: 239 PDP Addr       : 10.4.106.134 APN Used       : cmnet.mnc002.mcc460.gprs RNC Id         : 669\",\"logType\":\"session_event_log\",\"fileName\":\"188.1.92.106_session_event_log.4.txt\",\"logModule\":\"\",\"timenano\":10608626861853233,\"logLevel\":\"\",\"neNumber\":\"SGSN MKVIII\",\"netWork\":\"WCDMA\",\"node\":\"e_Erlang__Global_pm3_9_2_1@eqm03s09p2\",\"imsi\":\"460025120132401\",\"msisdn\":\"8618312019433\",\"sm_cause\":\"Network Failure (#38)\",\"details\":\"Unknown\",\"message\":\"\",\"pdp_addr\":\"10.4.106.134\",\"apn_used\":\"cmnet.mnc002.mcc460.gprs\",\"rnc_id\":\"669\",\"pdp_type\":\"\",\"eNodeB_id_type\":\"\",\"eNodeB_id\":\"\",\"eNodeB_plmn_id\":\"\",\"apn_req\":\"\",\"apn_sub\":\"\",\"pdn_addr\":\"\",\"sgw_addr\":\"\",\"k1\":\"Network Failure\"}";
        event.setBody(eventJson.getBytes());
        Context context = new Context();
        context.put(ES_INDEX, index);
        context.put(ES_TYPE, type);
        context.put(DELAY_DAYS, ""+delayDays);
        eventTimeBasedIndexBuilder.configure(context);
        String stdIndex = index + dateTime.toString("yyyy-MM-dd");
        assertEquals(stdIndex, eventTimeBasedIndexBuilder.getIndex(event));
        assertEquals(type, eventTimeBasedIndexBuilder.getType(event));
        assertEquals(delayDays, eventTimeBasedIndexBuilder.getDelayDays(event));
    }

    /**
     * tests Default index and type
     */
    @Test
    public void testDefaultIndex() {
        Event event = new SimpleEvent();
        assertNull(eventTimeBasedIndexBuilder.getIndex(event));
        assertEquals(DEFAULT_ES_TYPE, eventTimeBasedIndexBuilder.getType(event));
        assertEquals(DEFAULT_DELAY_DAYS, eventTimeBasedIndexBuilder.getDelayDays(event));
    }
}
