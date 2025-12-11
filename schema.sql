CREATE TABLE videos (
    id TEXT PRIMARY KEY,
    creator_id TEXT,
    video_created_at TIMESTAMP,
    views_count BIGINT,
    likes_count BIGINT,
    comments_count BIGINT,
    reports_count BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE video_snapshots (
    id TEXT PRIMARY KEY,
    video_id TEXT REFERENCES videos(id),
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