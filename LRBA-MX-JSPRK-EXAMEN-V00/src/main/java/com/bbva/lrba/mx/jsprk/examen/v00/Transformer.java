package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.mx.jsprk.examen.v00.model.JoinedRowData;
import com.bbva.lrba.mx.jsprk.examen.v00.model.MoviesRowData;
import com.bbva.lrba.mx.jsprk.examen.v00.model.StreamingRowData;
import com.bbva.lrba.spark.transformers.Transform;
import com.bbva.lrba.mx.jsprk.examen.v00.model.CharacteristicsRowData;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> characteristicDS = datasetsFromRead.get("sourceAlias1");
        Dataset<MoviesRowData> moviesDS = datasetsFromRead.get("sourceAlias2")
                .as(Encoders.bean(MoviesRowData.class));
        Dataset<StreamingRowData> streamingDS = datasetsFromRead.get("sourceAlias3")
                .as(Encoders.bean(StreamingRowData.class));

        Dataset<Row> moviesWstreamingDS = moviesDS.select(moviesDS.col("*"),
                functions.when(moviesDS.col("code").equalTo("NFLX"), "Netflix")
                        .when(moviesDS.col("code").equalTo("DSNY+"), "Disney+")
                        .when(moviesDS.col("code").equalTo("AMZN"), "Amazon Prime")
                        .alias("service"));


        System.out.println("Ej1");

        Dataset<Row> fullDS = moviesWstreamingDS.unionByName(characteristicDS, true);
        fullDS = fullDS.select(fullDS.col("*"),
                functions.lit(functions.current_timestamp()).alias("date"));

        fullDS.show();

        System.out.println("Ej2");

        Dataset<Row> MoviesPerService = fullDS.filter(functions.col("type").equalTo("Movie"))
                .groupBy("service").count().alias("Number of Movies");

        MoviesPerService.show();

        System.out.println("Ej3");

        Dataset<Row> directorsWMMPerStreaming = fullDS
                .groupBy("director").count().alias("Number of Movies");

        directorsWMMPerStreaming.sort("Number of Movies.director");

        directorsWMMPerStreaming.show();

        System.out.println("Ej4");

        Dataset<Row> LongestProductsPerStreaming = fullDS
                .filter(functions.col("duration").equalTo("10 Seasons")
                        .and(functions.col("duration").equalTo("90 min")));

        LongestProductsPerStreaming.show();

        System.out.println("Ej5");

        datasetsToWrite.put("targetAlias1", fullDS);
        datasetsToWrite.put("targetAlias2", MoviesPerService);
        datasetsToWrite.put("targetAlias3", directorsWMMPerStreaming);
        datasetsToWrite.put("targetAlias4", LongestProductsPerStreaming);
        //datasetsToWrite.put("targetAlias5", ejercicio5);

        return datasetsToWrite;
    }
}