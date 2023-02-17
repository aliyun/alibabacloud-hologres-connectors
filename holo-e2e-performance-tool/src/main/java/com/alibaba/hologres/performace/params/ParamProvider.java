package com.alibaba.hologres.performace.params;

public interface ParamProvider<T> {
  void init(String patter);

  T next();
}
