package org.apache.beam.runners.samza;

import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.jobsubmission.JobServerDriver;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SamzaJobServerDriver extends JobServerDriver {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobServerDriver.class);

  /** Samza runner-specific Configuration for the jobServer. */
  public static class SamzaServerConfiguration extends ServerConfiguration {

  }

  public static void main(String[] args) {
    // TODO: Expose the fileSystem related options.
    PortablePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(PortablePipelineOptions.class);
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    fromParams(args).run();
  }

  private static SamzaJobServerDriver fromParams(String[] args) {
    return fromConfig(parseArgs(args));
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.printf("Usage: java %s arguments...%n", SamzaJobServerDriver.class.getSimpleName());
    parser.printUsage(System.err);
    System.err.println();
  }

  private static SamzaJobServerDriver fromConfig(SamzaServerConfiguration configuration) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration));
  }

  public static SamzaServerConfiguration parseArgs(String[] args) {
    SamzaServerConfiguration configuration = new SamzaServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      printUsage(parser);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }
    return configuration;
  }

  private static SamzaJobServerDriver create(
      SamzaServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    return new SamzaJobServerDriver(
        configuration, jobServerFactory, artifactServerFactory);
  }

  private SamzaJobServerDriver(
      SamzaServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    super(configuration, jobServerFactory, artifactServerFactory, () -> SamzaJobInvoker.create(configuration));
  }
}
