CREATE TABLE videos (
    id BIGINT PRIMARY KEY,
    creator_id BIGINT,
    video_created_at TIMESTAMP,
    views_count BIGINT,
    likes_count BIGINT,
    comments_count BIGINT,
    reports_count BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE video_snapshots (
    id BIGINT PRIMARY KEY,
    video_id BIGINT REFERENCES videos(id),
    views_count BIGINT,
    likes_count BIGINT,
    comments_count BIGINT,
    reports_count BIGINT,
    delta_views_count BIGINT,
    delta_likes_count BIGINT,
    delta_comments_count BIGINT,
    delta_reports_count BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);