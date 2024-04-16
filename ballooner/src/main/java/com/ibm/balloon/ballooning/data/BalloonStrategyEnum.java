package com.ibm.balloon.ballooning.data;

import com.ibm.balloon.ballooning.data.domain.InputBalloonData;
import com.ibm.balloon.ballooning.data.knowledgebase.airlines.AirlinesConfig;
import com.ibm.balloon.ballooning.data.knowledgebase.airlines.Flight;
import com.ibm.balloon.ballooning.data.knowledgebase.gists.Gist;
import com.ibm.balloon.ballooning.data.knowledgebase.gists.GistConfig;
import com.ibm.balloon.ballooning.data.knowledgebase.movies.Movie;
import com.ibm.balloon.ballooning.data.knowledgebase.movies.MovieConfig;
import com.ibm.balloon.ballooning.data.knowledgebase.nasa.Meteorite;
import com.ibm.balloon.ballooning.data.knowledgebase.nasa.MeteoriteConfig;
import com.ibm.balloon.ballooning.data.knowledgebase.reddit.RedditListing;
import com.ibm.balloon.ballooning.data.knowledgebase.reddit.RedditConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum BalloonStrategyEnum {
    NASA(Meteorite.class, MeteoriteConfig.FILE_PATH),
    NASA_TEST(Meteorite.class, MeteoriteConfig.TEST_FILE_PATH),
    MOVIES(Movie.class, MovieConfig.FILE_PATH),
    AIRLINES(Flight.class, AirlinesConfig.FILE_PATH),
    GISTS(Gist.class, GistConfig.FILE_PATH),
    REDDIT(RedditListing.class, RedditConfig.FILE_PATH);

    private final Class<? extends InputBalloonData> dtoClazz;
    private final String knowledgeBaseFile;
}
