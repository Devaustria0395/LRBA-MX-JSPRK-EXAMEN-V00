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

        Dataset<CharacteristicsRowData> characteristicDS = datasetsFromRead.get("sourceAlias1")
                .as(Encoders.bean(CharacteristicsRowData.class));
        Dataset<MoviesRowData> moviesDS = datasetsFromRead.get("sourceAlias2")
                .as(Encoders.bean(MoviesRowData.class));
        Dataset<StreamingRowData> streamingDS = datasetsFromRead.get("sourceAlias3")
                .as(Encoders.bean(StreamingRowData.class));

        Dataset<Row> prejoinedDS = moviesDS.join(streamingDS,"code");
        prejoinedDS.show();

        Dataset<Row> joinedDS = characteristicDS.join(prejoinedDS,"title");

        System.out.println("Ej1");
        joinedDS = joinedDS.select(joinedDS.col("service")), functions.lit(functions.current_timestamp()).alias("date");

        joinedDS.show();

        datasetsToWrite.put("characteristicDS", characteristicDS.toDF());
        datasetsToWrite.put("moviesDS", moviesDS.toDF());
        datasetsToWrite.put("streamingDS", streamingDS.toDF());
        datasetsToWrite.put("joinedDS", joinedDS.toDF());

        return datasetsToWrite;
    }
}