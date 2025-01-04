
WITH video_comment_with_id AS
(
  SELECT
    comment.*,
    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY published_at) comment_id
  FROM {{ source('dev_schema', 'video_comment') }} as comment
  WHERE date_trunc('hour', to_timestamp(comment.created_at, 'YYYY-MM-DD HH24:MI:SS')) =
    (
        SELECT date_trunc('hour', max(to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS')))
        FROM {{ source('dev_schema', 'video_comment') }}
    )
)

SELECT
  video_id,
  comment_id,
  text_display,
  author_display_name,
  like_count,
  published_at,
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') created_at,
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') updated_at
FROM  video_comment_with_id
