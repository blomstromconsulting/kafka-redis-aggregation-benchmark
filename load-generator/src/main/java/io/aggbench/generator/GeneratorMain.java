package io.aggbench.generator;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/** Entry point of the load-generator Job: runs one benchmark run and exits (0 = drained, no send failures). */
@QuarkusMain
public class GeneratorMain implements QuarkusApplication {

    private static final Logger LOG = Logger.getLogger(GeneratorMain.class);

    @Inject
    GeneratorConfig config;

    @Inject
    MeterRegistry registry;

    public static void main(String... args) {
        Quarkus.run(GeneratorMain.class, args);
    }

    @Override
    public int run(String... args) {
        try {
            return new LoadGenerator(config, registry).run();
        } catch (Exception e) {
            LOG.error("Load generation failed", e);
            return 1;
        }
    }
}
