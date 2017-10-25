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

import static com.cognitree.flume.sink.elasticsearch.Constants.ID;
import static com.cognitree.flume.sink.elasticsearch.Constants.TYPE;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * Created by lij
 * <p>
 * This class create the index type and Id based on EventTime
 */
public class EventTimeBasedIndexBuilder extends StaticIndexBuilder {
	 private static final Logger logger = LoggerFactory.getLogger(EventTimeBasedIndexBuilder.class);
	 protected final ObjectReader reader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {});
	 
	 DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	 DateTimeFormatter YMDformatter = DateTimeFormat.forPattern("yyyy-MM-dd");
		
    /**
     * Returns the index name from the headers
     */
    @Override
    public String getIndex(Event event) {
    	String index = null;
    	String staticIndexName = super.getIndex(event);
    	index = getIndexName(staticIndexName,event);
    	return index;
    }

    /**
     * Returns the index type from the headers
     */
    @Override
    public String getType(Event event) {
        Map<String, String> headers = event.getHeaders();
        String type;
        if (headers.get(TYPE) != null) {
            type = headers.get(TYPE);
        } else {
            type = super.getType(event);
        }
        return type;
    }

    /**
     * Returns the index Id from the headers.
     */
    @Override
    public String getId(Event event) {
        Map<String, String> headers = event.getHeaders();
        return headers.get(ID);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);
    }
    
    /*
     * 根据eventTime获取索引名称，另外 对垃圾数据进行过滤
     * 过滤条件：city和domain不为空 并且 eventTime在三天之内
     */
	private String getIndexName(String staticIndexName, Event event) {
		logger.debug("############## in getIndexName" + new String(event.getBody(), Charsets.UTF_8));
		Map<String, Object> messageMap = null;
		try {
			messageMap = reader.readValue(event.getBody());
		} catch (JsonProcessingException e) {
			logger.error("get JsonProcessingException event=" + new String(event.getBody(), Charsets.UTF_8),e);
			return null;
		} catch (IOException e) {
			logger.error("get IOException event=" + new String(event.getBody(), Charsets.UTF_8),e);
			return null;
		}
		String eventTimeStr = (String) messageMap.get("eventTime");
		String cityStr = (String) messageMap.get("city");
		String domainStr = (String) messageMap.get("domain");
		
		if (StringUtils.isEmpty(cityStr) && StringUtils.isEmpty(domainStr)) {
			logger.warn("city and domain is empty, event=" + new String(event.getBody(), Charsets.UTF_8));
			return null;
		}
		
		if(StringUtils.isNotEmpty(eventTimeStr)) {
			try {
				long evnetTimeMS = formatter.parseMillis(eventTimeStr);
				DateTime eventTime = new DateTime(evnetTimeMS);
				int delayDays = getDelayDays(event);
				DateTime now = new DateTime();
				if (eventTime.isBefore(now) && eventTime.isAfter(now.minusDays(delayDays))) {
					return staticIndexName + YMDformatter.print(evnetTimeMS);
				} else {
					logger.warn("eventTime is not between now-[{}] and now event=" + new String(event.getBody(), Charsets.UTF_8),delayDays);
				}
			} catch(IllegalArgumentException e) {
				logger.error("get IllegalArgumentException event=" + new String(event.getBody(), Charsets.UTF_8),e);
			}
		} else {
			logger.warn("eventTime is empty, event=" + new String(event.getBody(), Charsets.UTF_8));
		}
		
		return null;
	}
	
	public static void main(String[] args) {
		String eventTimeStr = "2017-10-22 10:49:22";
		 DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		long evnetTimeMS = formatter.parseMillis(eventTimeStr);
		DateTime eventTime = new DateTime(evnetTimeMS);
		DateTime now = new DateTime();
		System.out.println(eventTime.isAfter(now.minusDays(3)));
	}
}
