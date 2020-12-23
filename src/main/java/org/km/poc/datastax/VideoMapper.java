package org.km.poc.datastax;

import com.datastax.oss.driver.api.core.cql.Row;

public class VideoMapper {

    public Video map(Row row) {
        Video vd = new Video();
        vd.setVideoId(row.getUuid("video_id"));
        vd.setAddedDate(row.getInstant("added_date"));
        vd.setTitle(row.getString("title"));
        return vd;
    }

}
