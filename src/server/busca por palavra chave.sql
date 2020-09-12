SELECT A.VideoPK, A.ChannelPK, A.ScoreVideo, A.ScoreChannel
    ,V.DislikeCount AS DislikeCountVideo
    ,V.LikeCount AS LikeCountVideo
    ,V.Date AS DateVideo
    ,V.Rating AS RatingVideo
    ,V.Duration AS DurationVideo
    ,V.ViewCount AS ViewCountVideo
    ,C.DislikeCount AS DislikeCountChannel
    ,C.LikeCount AS LikeCountChannel
    ,C.Rating AS RatingChannel
    ,C.Duration AS DurationChannel
    ,C.ViewCount AS ViewCountChannel
    ,C.ViewCount AS ViewCountChannel
FROM
    (
        SELECT WV.VideoPK, WV.ChannelPK, SUM(WC.Score) AS ScoreChannel, SUM(WV.Score) AS ScoreVideo
        FROM
            dictionary D
            ,word_by_video WV
            ,word_by_channel WC
        WHERE
            D.Word IN ('best', 'video', 'music', 'film', 'exercises', 'friend', 'feeling', 'best video', 'video music', 'music film', 'film exercises', 'exercises friend', 'friend feeling', 'best video music', 'video music film', 'music film exercises', 'film exercises friend', 'exercises friend feeling', 'best video music film', 'video music film exercises', 'music film exercises friend', 'film exercises friend feeling', 'best video music film exercises', 'video music film exercises friend', 'music film exercises friend feeling')
            AND D.WordPK = WV.WordPK
            AND WV.WordPK = WC.WordPK
            AND WV.ChannelPK = WC.ChannelPK
        GROUP BY
            WV.VideoPK, WV.ChannelPK
    ) A
    ,video V
    ,channel C
WHERE
    A.VideoPK = V.VideoPK
    AND A.ChannelPK = C.ChannelPK
ORDER BY A.VideoPK