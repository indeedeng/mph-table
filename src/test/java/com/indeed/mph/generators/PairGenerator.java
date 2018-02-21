package com.indeed.mph.generators;

import com.indeed.util.core.Pair;
import com.pholser.junit.quickcheck.generator.ComponentizedGenerator;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class PairGenerator extends ComponentizedGenerator<Pair> {

    private static final int FIRST = 0;
    private static final int SECOND = 1;

    public PairGenerator() {
        super(Pair.class);
    }

    @Override
    public Pair generate(final SourceOfRandomness random, final GenerationStatus status) {
        return Pair.of(
                componentGenerators().get(FIRST).generate(random, status),
                componentGenerators().get(SECOND).generate(random, status)
        );
    }

    @Override
    public int numberOfNeededComponents() {
        return 2;
    }

    @Override
    public List<Pair> doShrink(final SourceOfRandomness random, final Pair larger) {
        final List<?> shrunkenFirsts = componentGenerators().get(FIRST).shrink(random, larger.getFirst());
        final List<?> shrunkenSeconds = componentGenerators().get(SECOND).shrink(random, larger.getSecond());

        return IntStream.range(0, Math.min(shrunkenFirsts.size(), shrunkenSeconds.size()))
                .mapToObj(i -> Pair.of(shrunkenFirsts.get(i), shrunkenSeconds.get(i)))
                .collect(toList());
    }
}
