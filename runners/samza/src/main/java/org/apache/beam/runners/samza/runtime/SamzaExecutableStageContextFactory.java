package org.apache.beam.runners.samza.runtime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.fnexecution.control.DefaultExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;

/**
 * Singleton class that contains one {@link ExecutableStageContext.Factory} per job. Assumes it is
 * safe to release the backing environment asynchronously.
 */
public class SamzaExecutableStageContextFactory implements ExecutableStageContext.Factory {

  private static final SamzaExecutableStageContextFactory instance =
      new SamzaExecutableStageContextFactory();
  // This map should only ever have a single element, as each job will have its own
  // classloader and therefore its own instance of FlinkExecutableStageContextFactory. This
  // code supports multiple JobInfos in order to provide a sensible implementation of
  // Factory.get(JobInfo), which in theory could be called with different JobInfos.
  private static final ConcurrentMap<String, ExecutableStageContext.Factory> jobFactories =
      new ConcurrentHashMap<>();

  private SamzaExecutableStageContextFactory() {}

  public static SamzaExecutableStageContextFactory getInstance() {
    return instance;
  }

  @Override
  public ExecutableStageContext get(JobInfo jobInfo) {
    ExecutableStageContext.Factory jobFactory =
        jobFactories.computeIfAbsent(
            jobInfo.jobId(),
            k -> ReferenceCountingExecutableStageContextFactory.create(
                DefaultExecutableStageContext::create,
                // Always release environment asynchronously.
                (caller) -> false));

    return jobFactory.get(jobInfo);
  }
}
