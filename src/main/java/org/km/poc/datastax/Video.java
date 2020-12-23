package org.km.poc.datastax;

import java.time.Instant;
import java.util.UUID;

public class Video {
    private UUID videoId;
    private Instant addedDate;
    private String title;

    public UUID getVideoId() {
        return videoId;
    }

    public void setVideoId(UUID videoId) {
        this.videoId = videoId;
    }

    public Instant getAddedDate() {
        return addedDate;
    }

    public void setAddedDate(Instant addedDate) {
        this.addedDate = addedDate;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ID: ")
            .append(videoId)
            .append(" Added date: ")
            .append(addedDate)
            .append(" Title: ")
            .append(title);

        return sb.toString();
    }
}
