
WITH video_comment_with_id AS
(
  SELECT
    comment.*,
    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY published_at) comment_id
  FROM {{ source('dev', 'video_comment') }} as comment
  WHERE DATE_TRUNC('HOUR', comment.created_at) =
    (
        SELECT DATE_TRUNC('HOUR', MAX(created_at))
        FROM {{ source('dev', 'video_comment') }}
    )
)

SELECT
  video_id,
  comment_id,
  text_display,
  author_display_name,
  like_count,
  published_at,
  DATE_TRUNC('SECOND', CURRENT_TIMESTAMP) created_at,
  DATE_TRUNC('SECOND', CURRENT_TIMESTAMP) updated_at
FROM  video_comment_with_id
