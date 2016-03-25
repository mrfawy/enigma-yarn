package com.tito.easyyarn.appmaster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.tito.easyyarn.appmaster.ApplicationMaster.DSEntity;
import com.tito.easyyarn.appmaster.ApplicationMaster.DSEvent;

public class TimeLinePublisher {
	private TimelineClient timelineClient;

	public TimeLinePublisher(Configuration conf) {
		timelineClient = TimelineClient.createTimelineClient();
		timelineClient.init(conf);
		timelineClient.start();

	}

	public void publishApplicationAttemptEvent(String appAttemptId, DSEvent appEvent)
			throws IOException, YarnException {
		TimelineEntity entity = new TimelineEntity();
		entity.setEntityId(appAttemptId);
		entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
		entity.addPrimaryFilter("user", UserGroupInformation.getCurrentUser().getShortUserName());
		TimelineEvent event = new TimelineEvent();
		event.setEventType(appEvent.toString());
		event.setTimestamp(System.currentTimeMillis());
		entity.addEvent(event);

		timelineClient.putEntities(entity);
	}
	public void publishContainerStartEvent(Container container)
			throws IOException, YarnException {
		TimelineEntity entity = new TimelineEntity();
	    entity.setEntityId(container.getId().toString());
	    entity.setEntityType(DSEntity.DS_CONTAINER.toString());
	    entity.addPrimaryFilter("user",
	        UserGroupInformation.getCurrentUser().getShortUserName());
	    TimelineEvent event = new TimelineEvent();
	    event.setTimestamp(System.currentTimeMillis());
	    event.setEventType(DSEvent.DS_CONTAINER_START.toString());
	    event.addEventInfo("Node", container.getNodeId().toString());
	    event.addEventInfo("Resources", container.getResource().toString());
	    entity.addEvent(event);

	    timelineClient.putEntities(entity);
	}
	public void publishContainerEndEvent(ContainerStatus container)
			throws IOException, YarnException {
		TimelineEntity entity = new TimelineEntity();
		entity.setEntityId(container.getContainerId().toString());
		entity.setEntityType(DSEntity.DS_CONTAINER.toString());
		entity.addPrimaryFilter("user", UserGroupInformation.getCurrentUser().getShortUserName());
		TimelineEvent event = new TimelineEvent();
		event.setTimestamp(System.currentTimeMillis());
		event.setEventType(DSEvent.DS_CONTAINER_END.toString());
		event.addEventInfo("State", container.getState().name());
		event.addEventInfo("Exit Status", container.getExitStatus());
		entity.addEvent(event);

		timelineClient.putEntities(entity);
	}
}
