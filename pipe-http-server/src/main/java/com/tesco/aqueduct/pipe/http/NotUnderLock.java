package com.tesco.aqueduct.pipe.http;

import java.util.Arrays;
import java.util.concurrent.FutureTask;

public class NotUnderLock {
  FutureTask<Object> future = null;

  public void callFutureSetOk() {
    future.set();
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
