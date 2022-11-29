package com.tesco.aqueduct.pipe.http;

import java.util.concurrent.FutureTask;

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
}
