package com.tesco.aqueduct.pipe.http;

import com.google.common.collect.ImmutableList;
import jakarta.inject.Singleton;

import java.util.Arrays;
import java.util.concurrent.FutureTask;

@Singleton
public class NotUnderLock {
  FutureTask<Object> future = null;

  public void callFutureSetOk() {
    future.run();
  }

  public synchronized void firstAcquisitionBad() {
    callFutureSetOk();
  }

  public void secondAcquisitionOk(Object o) {
    synchronized (o) {
      firstAcquisitionBad();
    }
  }

  public void triggerErrorProne() {
      String[] foo = new String[42];
      Arrays.fill(foo, 42);
  }
}
