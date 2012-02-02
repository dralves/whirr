package org.apache.whirr.service;

public class ServiceNotFoundException extends RuntimeException {

  public ServiceNotFoundException(String name) {
    super("Could not find service: " + name);
  }

}
