package org.apache.beam.runners.samza;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.runners.jobsubmission.JobInvoker;
import org.apache.beam.runners.jobsubmission.PortablePipelineJarCreator;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SamzaJobInvoker extends JobInvoker {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobInvoker.class);
  private final SamzaJobServerDriver.SamzaServerConfiguration configuration;

  public static SamzaJobInvoker create(
      SamzaJobServerDriver.SamzaServerConfiguration configuration) {
    return new SamzaJobInvoker(configuration);
  }

  private SamzaJobInvoker(SamzaJobServerDriver.SamzaServerConfiguration configuration) {
    super("samza-runner-job-invoker-%d");
    this.configuration = configuration;
  }

  @Override
  protected JobInvocation invokeWithExecutor(
      RunnerApi.Pipeline pipeline,
      Struct options,
      @Nullable String retrievalToken,
      ListeningExecutorService executorService) {
    LOG.trace("Parsing pipeline options");
    SamzaPortablePipelineOptions samzaOptions =
        PipelineOptionsTranslation.fromProto(options).as(SamzaPortablePipelineOptions.class);
    Map<String, String> overrideConfig =
        samzaOptions.getConfigOverride() != null
            ? samzaOptions.getConfigOverride()
            : new HashMap<>();
    overrideConfig.put(SamzaRunnerOverrideConfigs.IS_PORTABLE_MODE, String.valueOf(true));
    overrideConfig.put(
        SamzaRunnerOverrideConfigs.FN_CONTROL_PORT,
        String.valueOf(samzaOptions.getControlPort()));
    overrideConfig.put(SamzaRunnerOverrideConfigs.FS_TOKEN_PATH, samzaOptions.getFsTokenPath());
    samzaOptions.setConfigOverride(overrideConfig);
    // Options can't be translated to proto if runner class is unresolvable, so set it to null.
    samzaOptions.setRunner(null);

    PortablePipelineRunner pipelineRunner;
    if (Strings.isNullOrEmpty(samzaOptions.as(PortablePipelineOptions.class).getOutputExecutablePath())) {
      pipelineRunner = new SamzaPipelineRunner(samzaOptions);
    } else {
      pipelineRunner = new PortablePipelineJarCreator(SamzaPipelineRunner.class);
    }

    String invocationId =
        String.format("%s_%s", samzaOptions.getJobName(), UUID.randomUUID().toString());
    return createJobInvocation(
        invocationId, retrievalToken, executorService, pipeline, samzaOptions, pipelineRunner);
  }

  protected JobInvocation createJobInvocation(
      String invocationId,
      String retrievalToken,
      ListeningExecutorService executorService,
      RunnerApi.Pipeline pipeline,
      SamzaPipelineOptions samzaOptions,
      PortablePipelineRunner pipelineRunner
  ) {
    JobInfo jobInfo =
        JobInfo.create(
            invocationId,
            samzaOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(samzaOptions));
    return new JobInvocation(jobInfo, executorService, pipeline, pipelineRunner);
  }
}
