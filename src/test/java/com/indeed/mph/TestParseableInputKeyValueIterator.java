 package com.indeed.mph;

 import com.google.common.collect.ImmutableMap;
 import com.indeed.mph.serializers.*;
 import com.indeed.util.core.Pair;
 import org.junit.Assert;
 import org.junit.Test;

 import java.io.IOException;
 import java.io.InputStream;
 import java.util.Iterator;
 import java.util.List;
 import java.util.Map;

 /**
  * @author xinjianz
  */
 public class TestParseableInputKeyValueIterator {
     @Test
     public void testReplace() throws IOException {
         final SmartStringConcatSerializer clusterNameSerializer = new SmartStringConcatSerializer(
                 new SmartStringSerializer(), new SmartDictionarySerializer(), " ");
         final SmartListSerializer<Long> jobListSerializer = new SmartListSerializer<>(new SmartLongSerializer());
         try (final InputStream in = this.getClass().getClassLoader().getResourceAsStream("input/mph/clusterToJobs.txt")) {
             final Iterator<Pair<String, List<Long>>> iterator = new ParseableInputKeyValueIterator<>(
                     in, clusterNameSerializer , jobListSerializer, "\t", "[{}()]+", "", true, true
             );
             final Map<String, String> expectedTuples = ImmutableMap.of(
                     "9qh4 chef", "[4320836901, 4344529077, 4145418399]",
                     "dnen buyer", "[4313223019]",
                     "dpgs (nurse)", "[4306822941]",
                     "dpgs software engineer", "[4306822947]");
             while (iterator.hasNext()) {
                 final Pair<String, List<Long>> tuple = iterator.next();
                 Assert.assertEquals(expectedTuples.get(tuple.getFirst()), tuple.getSecond().toString());
             }
         }
     }
 }
