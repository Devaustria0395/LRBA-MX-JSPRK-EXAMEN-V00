package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobExamenBuilderTest {

    private JobExamenBuilder jobExamenBuilder;

    @BeforeEach
    void setUp() {
        this.jobExamenBuilder = new JobExamenBuilder();
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobExamenBuilder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(3, sourcesList.getSources().size());

        final Source source = sourcesList.getSources().get(0);
        assertNotNull(source);
        assertEquals("sourceAlias1", source.getAlias());
        assertEquals("characteristics.csv", source.getPhysicalName());

        final Source source2 = sourcesList.getSources().get(1);
        assertNotNull(source);
        assertEquals("sourceAlias2", source.getAlias());
        assertEquals("movies.csv", source.getPhysicalName());

        final Source source3 = sourcesList.getSources().get(2);
        assertNotNull(source);
        assertEquals("sourceAlias3", source.getAlias());
        assertEquals("streaming.csv", source.getPhysicalName());
    }

    @Test
    void registerTransform_na_Transform() {
        //IF YOU WANT TRANSFORM CLASS
        final TransformConfig transformConfig = this.jobExamenBuilder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
        //IF YOU WANT SQL TRANSFORM
        //final TransformConfig transformConfig = this.jobExamenBuilder.registerTransform();
        //assertNotNull(transformConfig);
        //assertNotNull(transformConfig.getTransformSqls());
        //IF YOU DO NOT WANT TRANSFORM
        //final TransformConfig transformConfig = this.jobExamenBuilder.registerTransform();
        //assertNull(transformConfig);
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobExamenBuilder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(4, targetsList.getTargets().size());

        final Target target = targetsList.getTargets().get(3);
        assertNotNull(target);
        assertEquals("joinedDS", target.getAlias());
        assertEquals("output/joinedDS.csv", target.getPhysicalName());
    }

}