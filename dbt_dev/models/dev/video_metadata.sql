
WITH popular_video_with_category AS
(
  SELECT
    video.*,
    category.category_name
  FROM {{ source('dev', 'popular_video') }} as video
    LEFT JOIN {{ source('dev', 'video_category') }} as category
      ON video.category_id = category.category_id
  WHERE date_trunc('hour', to_timestamp(video.created_at, 'YYYY-MM-DD HH24:MI:SS')) =
    (
        SELECT date_trunc('hour', max(to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS')))
        FROM {{ source('dev', 'popular_video') }}
    )
)

SELECT
  video_id,
  title,
  category_name,
  thumbnail_url,
  comment_count,
  like_count,
  view_count,
  published_at,
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') created_at,
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') updated_at
FROM popular_video_with_category
