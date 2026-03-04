-- Таблица итоговой статистики по каждому видео (id/creator_id — UUID/hex в боевых данных)
CREATE TABLE videos (
    id UUID PRIMARY KEY,
    creator_id TEXT NOT NULL,
    video_created_at TIMESTAMPTZ NOT NULL,
    views_count BIGINT NOT NULL DEFAULT 0,
    likes_count BIGINT NOT NULL DEFAULT 0,
    comments_count BIGINT NOT NULL DEFAULT 0,
    reports_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_videos_creator_id ON videos (creator_id);
CREATE INDEX idx_videos_video_created_at ON videos (video_created_at);

-- Почасовые снапшоты статистики по каждому видео
CREATE TABLE video_snapshots (
    id BIGINT PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos (id) ON DELETE CASCADE,
    views_count BIGINT NOT NULL DEFAULT 0,
    likes_count BIGINT NOT NULL DEFAULT 0,
    comments_count BIGINT NOT NULL DEFAULT 0,
    reports_count BIGINT NOT NULL DEFAULT 0,
    delta_views_count BIGINT NOT NULL DEFAULT 0,
    delta_likes_count BIGINT NOT NULL DEFAULT 0,
    delta_comments_count BIGINT NOT NULL DEFAULT 0,
    delta_reports_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_video_snapshots_video_id ON video_snapshots (video_id);
CREATE INDEX idx_video_snapshots_created_at ON video_snapshots (created_at);
