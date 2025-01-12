
WITH popular_video_with_category AS
(
  SELECT
    video.*,
    category.category_name
  FROM {{ source('dev', 'popular_video') }} as video
    LEFT JOIN {{ source('dev', 'video_category') }} as category
      ON video.category_id = category.category_id
  WHERE DATE_TRUNC('HOUR', video.created_at) =
    (
        SELECT DATE_TRUNC('HOUR', MAX(created_at))
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
  DATE_TRUNC('SECOND', CURRENT_TIMESTAMP) created_at,
  DATE_TRUNC('SECOND', CURRENT_TIMESTAMP) updated_at
FROM popular_video_with_category
