package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.PerformanceStreamConfig;
import com.factory.message.GasComposition;
import lombok.Builder;

public class ReactionPerformanceStreamFactory extends PerformanceStreamFactory<GasComposition> {

    @Builder
    public ReactionPerformanceStreamFactory(final KafkaNativeConfig kafkaNativeConfig, final PerformanceStreamConfig performanceStreamConfig) {
        super(kafkaNativeConfig, performanceStreamConfig);
    }

    @Override
    protected float calculatePerformance(final GasComposition input, final GasComposition output) {
        // 3H2 + N2 -> 2NH3
        final int h2Coefficient = 3;
        final int n2Coefficient = 1;
        final int nh3Coefficient = 2;

        double limitElement = Math.min(input.getData().getH2() / h2Coefficient, input.getData().getN2() / n2Coefficient);
        double theoreticalNH3 = limitElement * nh3Coefficient;

        return (float) (output.getData().getNh3() / theoreticalNH3);
    }
}
