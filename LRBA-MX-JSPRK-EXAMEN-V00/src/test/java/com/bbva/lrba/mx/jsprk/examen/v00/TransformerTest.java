package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.mx.jsprk.examen.v00.model.JoinedRowData;
import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import com.bbva.lrba.mx.jsprk.examen.v00.model.CharacteristicsRowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType Characteristicsschema = DataTypes.createStructType(
               new StructField[]{
                         DataTypes.createStructField("title", DataTypes.StringType, true),
                         DataTypes.createStructField("country", DataTypes.StringType, true),
                         DataTypes.createStructField("dateAdded", DataTypes.StringType, true),
                         DataTypes.createStructField("releaseYear", DataTypes.StringType, true),
                         DataTypes.createStructField("rating", DataTypes.StringType, true),
                         DataTypes.createStructField("duration", DataTypes.StringType, true),
                         DataTypes.createStructField("listedIn", DataTypes.StringType, true),
               });
        Row cfirstRow = RowFactory.create("Dick Johnson Is Dead","United States","September 25, 2021","2020","PG-13",
                "90 min","Documentaries");
        Row csecondRow = RowFactory.create("Blood & Water","South Africa","September 24, 2021","2021","TV-MA","2 Seasons",
                "International TV Shows, TV Dramas, TV Mysteries");
        Row cthirdRow = RowFactory.create("Ganglands","","September 24, 2021","2021","TV-MA","1 Season",
                "Crime TV Shows, International TV Shows, TV Action & Adventure");

        StructType Moviesschema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("title", DataTypes.StringType, true),
                        DataTypes.createStructField("director", DataTypes.StringType, true),
                        DataTypes.createStructField("cast", DataTypes.StringType, true),
                        DataTypes.createStructField("description", DataTypes.StringType, true),
                        DataTypes.createStructField("code", DataTypes.StringType, true),
                        DataTypes.createStructField("type", DataTypes.StringType, true),
                });
        Row mfirstRow = RowFactory.create("Dick Johnson Is Dead","Kirsten Johnson","",
                "As her father nears the end of his life, filmmaker Kirsten Johnson stages his death in inventive and comical ways to help them both face the inevitable.",
                "NFLX","Movie");
        Row msecondRow = RowFactory.create("Blood & Water","",
                "Ama Qamata, Khosi Ngema, Gail Mabalane, Thabang Molaba, Dillon Windvogel, Natasha Thahane, Arno Greeff, Xolile Tshabalala, Getmore Sithole, Cindy Mahlangu, Ryle De Morny, Greteli Fincham, Sello Maake Ka-Ncube, Odwa Gwanya, Mekaila Mathys, Sandi Schultz, Duane Williams, Shamilla Miller, Patrick Mofokeng",
                "After crossing paths at a party, a Cape Town teen sets out to prove whether a private-school swimming star is her sister who was abducted at birth.",
                "NFLX","TV Show");
        Row mthirdRow = RowFactory.create("Ganglands","Julien Leclercq",
                "Sami Bouajila, Tracy Gotoas, Samuel Jouy, Nabiha Akkari, Sofia Lesaffre, Salim Kechiouche, Noureddine Farihi, Geert Van Rampelberg, Bakary Diombera",
                "To protect his family from a powerful drug lord, skilled thief Mehdi and his expert team of robbers are pulled into a violent and deadly turf war.",
                "NFLX","TV Show");

        StructType Streamingschema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("code", DataTypes.StringType, true),
                        DataTypes.createStructField("service", DataTypes.StringType, true),
                });

        Row sfirstRow = RowFactory.create("NFLX","Netflix");
        Row ssecondRow = RowFactory.create("DSNY+","Disney+");
        Row sthirdRow = RowFactory.create("AMZN","Amazon Prime");

        final List<Row> clistRows = Arrays.asList(cfirstRow, csecondRow, cthirdRow);
        final List<Row> mlistRows = Arrays.asList(mfirstRow, msecondRow, mthirdRow);
        final List<Row> slistRows = Arrays.asList(sfirstRow, ssecondRow, sthirdRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> cdataset = datasetUtils.createDataFrame(clistRows, Characteristicsschema);
        Dataset<Row> mdataset = datasetUtils.createDataFrame(mlistRows, Moviesschema);
        Dataset<Row> sdataset = datasetUtils.createDataFrame(slistRows, Streamingschema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>
                (Map.of("sourceAlias1", cdataset, "sourceAlias2", mdataset,"sourceAlias3",sdataset)));

        assertNotNull(datasetMap);
        assertEquals(4, datasetMap.size());

        Dataset<JoinedRowData> returnedDs = datasetMap.get("joinedDS").as(Encoders.bean(JoinedRowData.class));
        final List<JoinedRowData> rows = datasetToTargetData(returnedDs, JoinedRowData.class);

    }

}